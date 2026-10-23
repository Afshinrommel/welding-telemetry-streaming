import os
import numpy as np
import ufl
import json  # اضافه شدن کتابخانه json برای ذخیره فایل

from petsc4py import PETSc
from dolfinx import fem, io
from dolfinx.fem.petsc import LinearProblem
from postprocessor import compute_stress_fields

from material import (
    youngs_modulus,
    poisson_ratio,
    expansion_coefficient,
    yield_strength,
    hardening_modulus
)

# ============================================================
# Tensor utilities
# ============================================================

def eps(u):
    """Small-strain tensor."""
    return ufl.sym(ufl.grad(u))

def dev(A):
    """Deviatoric part of a 3x3 tensor."""
    return A - ufl.tr(A) / 3.0 * ufl.Identity(3)

def make_dg0_space(mesh, value_shape=()):
    """Create DG0 scalar or tensor space."""
    if value_shape == ():
        return fem.functionspace(mesh, ("DG", 0))

    return fem.functionspace(
        mesh,
        ("DG", 0, value_shape),
    )

# ============================================================
# Mechanical boundary conditions
# ============================================================

def create_mechanical_bcs(mesh, V, cfg):

    L = float(cfg.length)
    W = float(cfg.width)

    points = [
        np.array([0.0, 0.0, 0.0]),
        np.array([L,   0.0, 0.0]),
        np.array([0.0, W,   0.0]),
        np.array([L,   W,   0.0]),
    ]

    bcs = []

    for pnt in points:
        def marker(x, p=pnt):
            return np.logical_and.reduce(
                np.isclose(x.T, p, atol=1.0e-8),
                axis=1,
            )

        dofs = fem.locate_dofs_geometrical(V, marker)

        if len(dofs) > 0:
            zero = np.zeros(3, dtype=PETSc.ScalarType)
            bcs.append(fem.dirichletbc(zero, dofs, V))

    return bcs

# ============================================================
# NumPy J2 radial return with isotropic hardening
# ============================================================

def j2_radial_return(
    sigma_trial,
    sigma_y,
    mu,
    H,
):
    """
    J2 von-Mises radial return with linear isotropic hardening.
    """
    n_cells = sigma_trial.shape[0]
    identity = np.eye(3)

    p_trial = np.trace(sigma_trial, axis1=1, axis2=2) / 3.0
    s_trial = sigma_trial - p_trial[:, None, None] * identity[None, :, :]
    
    seq_trial = np.sqrt(
        np.maximum(
            0.0,
            1.5 * np.sum(s_trial * s_trial, axis=(1, 2)),
        )
    )

    d_eps_p = np.zeros_like(sigma_trial)
    d_eqp = np.zeros(n_cells, dtype=float)
    sigma_new = sigma_trial.copy()

    active = (seq_trial > sigma_y) & (seq_trial > 1.0e-14)
    active_ids = np.where(active)[0]

    if len(active_ids) > 0:
        delta_eqp = (seq_trial[active_ids] - sigma_y[active_ids]) / (
            3.0 * mu[active_ids] + H[active_ids]
        )
        delta_eqp = np.maximum(delta_eqp, 0.0)
        d_eqp[active_ids] = delta_eqp

        d_eps_p[active_ids] = (
            1.5
            * delta_eqp[:, None, None]
            * s_trial[active_ids]
            / seq_trial[active_ids, None, None]
        )

        seq_new = seq_trial[active_ids] - 3.0 * mu[active_ids] * delta_eqp
        seq_new = np.maximum(seq_new, 0.0)

        s_new_active = (seq_new / seq_trial[active_ids])[:, None, None] * s_trial[active_ids]

        sigma_new[active_ids] = (
            p_trial[active_ids, None, None] * identity[None, :, :] + s_new_active
        )

    return d_eps_p, d_eqp, sigma_new

# ============================================================
# Mechanical transient solver
# ============================================================

def solve_mechanics_transient(
    cfg,
    mesh,
    thermal_history,
    out_dir,
    cell_tags=None,
    exporter=None,
):
    comm = mesh.comm
    T0 = float(cfg.T0)

    plastic_inner_tol = float(getattr(cfg, "plastic_inner_tol", 1.0e-6))
    plastic_inner_max_it = int(getattr(cfg, "plastic_inner_max_it", 30))
    plastic_relaxation = float(getattr(cfg, "plastic_relaxation", 0.7))
    mechanics_ksp_type = str(getattr(cfg, "mechanics_ksp_type", "preonly"))
    mechanics_pc_type = str(getattr(cfg, "mechanics_pc_type", "lu"))

    H0 = float(getattr(cfg, "hardening_modulus", 1.0e9))
    H_min_factor = float(getattr(cfg, "hardening_min_factor", 0.10))
    H_T_ref = float(getattr(cfg, "hardening_T_ref", 400.0))
    H_T_end = float(getattr(cfg, "hardening_T_end", 1100.0))

    # ارتقای المان جابجایی به مرتبه دوم (P2) برای دقت خمش بالا
    V = fem.functionspace(mesh, ("Lagrange", 1, (3,))) 
    Q = fem.functionspace(mesh, ("Lagrange", 1))
    Qp_tensor = make_dg0_space(mesh, (3, 3))
    Qp_scalar = make_dg0_space(mesh)

    # Gum Phase tracking fields on DG0 space
    is_bead_mech = fem.Function(Qp_scalar)
    activated_mech = fem.Function(Qp_scalar)
    activated_mech.name = "MaterialState"
    activated_mech.x.array[:] = 1.0
    is_bead_mech.x.array[:] = 0.0

    if cell_tags is not None:
        bead_cells = cell_tags.find(1)
        if len(bead_cells) > 0:
            is_bead_mech.x.array[bead_cells] = 1.0
            activated_mech.x.array[bead_cells] = 0.0
            print(f"-> Mechanical Gum Phase: {len(bead_cells)} weld bead cells set to inactive.")
        else:
            print("-> WARNING: Tag 2 (WeldBead) was not found in cell_tags!")

    u = fem.Function(V, name="Displacement")
    T_curr = fem.Function(Q, name="Temperature")
    T_prev = fem.Function(Q, name="TemperaturePrevious")
    eps_p = fem.Function(Qp_tensor, name="PlasticStrain")
    eps_th = fem.Function(Qp_tensor, name="ThermalStrain")
    eqp = fem.Function(Qp_scalar, name="EquivalentPlasticStrain")

    T_curr.x.array[:] = T0
    T_prev.x.array[:] = T0
    eps_p.x.array[:] = 0.0
    eps_th.x.array[:] = 0.0
    eqp.x.array[:] = 0.0

    T_curr.x.scatter_forward()
    T_prev.x.scatter_forward()
    eps_p.x.scatter_forward()
    eps_th.x.scatter_forward()
    eqp.x.scatter_forward()

    v = ufl.TestFunction(V)
    du = ufl.TrialFunction(V)
    dx = ufl.Measure("dx", domain=mesh)

    # Dynamic elastic functions for global stiffness matrix matching gum phase
    mu_func = fem.Function(Qp_scalar, name="Mu")
    lam_func = fem.Function(Qp_scalar, name="Lambda")

    def sigma_elastic(strain):
        return lam_func * ufl.tr(strain) * ufl.Identity(3) + 2.0 * mu_func * strain

    a = ufl.inner(sigma_elastic(eps(du)), eps(v)) * dx
    bcs = create_mechanical_bcs(mesh, V, cfg)

    os.makedirs(out_dir, exist_ok=True)
    filename = os.path.join(out_dir, "mechanical_elastoplastic.xdmf")
    writer = io.XDMFFile(comm, filename, "w")
    writer.write_mesh(mesh)

    n_steps = len(thermal_history)
    
    # تعریف بافر برای نگهداری داده‌ها در حافظه
    results_buffer = [] 

    for step, (t_val, Tarr) in enumerate(thermal_history):

        if comm.rank == 0:
            print(f"\n======================================================================")
            print(f"Mechanical step {step + 1}/{n_steps}")
            print(f"time = {t_val:.6g}")

        T_curr.x.array[:] = Tarr
        T_curr.x.scatter_forward()
        T_prev.x.scatter_forward()

        # Interpolate temperature to DG0 for gum phase and elastic properties update
        T_dg_expr = fem.Expression(T_curr, Qp_scalar.element.interpolation_points)
        T_dg = fem.Function(Qp_scalar)
        T_dg.interpolate(T_dg_expr)
        T_dg.x.scatter_forward()
        T_dg_np = T_dg.x.array

        if cell_tags is not None:
            activation_mask = (is_bead_mech.x.array > 0.5) & (T_dg_np >= 500.0) & (activated_mech.x.array < 0.5)
            if np.any(activation_mask):
                activated_mech.x.array[activation_mask] = 1.0
                activated_mech.x.scatter_forward()
            
            gum_factor_np = np.where(is_bead_mech.x.array > 0.5, np.where(activated_mech.x.array > 0.5, 1.0, 1e-4), 1.0)
        else:
            gum_factor_np = 1.0

        # Update global elastic properties matching gum phase
        E_np = youngs_modulus(T_dg_np) * gum_factor_np
        nu_np = poisson_ratio(T_dg_np)
        mu_np = E_np / (2.0 * (1.0 + nu_np))
        lam_np = E_np * nu_np / ((1.0 + nu_np) * (1.0 - 2.0 * nu_np))

        mu_func.x.array[:] = mu_np
        mu_func.x.scatter_forward()
        lam_func.x.array[:] = lam_np
        lam_func.x.scatter_forward()

        dT = T_curr - T_prev
        d_eps_th_expr = expansion_coefficient(T_curr) * dT * ufl.Identity(3)
        d_eps_th = fem.Function(Qp_tensor)
        d_eps_th.interpolate(fem.Expression(d_eps_th_expr, Qp_tensor.element.interpolation_points))
        d_eps_th.x.scatter_forward()
        eps_th.x.array[:] += d_eps_th.x.array
        eps_th.x.scatter_forward()

        # ====================================================
        # Metallurgical Annealing (Reset Hardening History)
        # ====================================================
        T_anneal = float(getattr(cfg, "T_anneal", 1000.0))
        anneal_mask = T_dg.x.array > T_anneal
        
        if np.any(anneal_mask):
            n_annealed = np.sum(anneal_mask)
            if comm.rank == 0:
                print(f"  [Annealing] Erased hardening history for {n_annealed} cells (T > {T_anneal}°C)")
                
            eqp.x.array[anneal_mask] = 0.0
            eqp.x.scatter_forward()

        # ====================================================
        # State at beginning of physical timestep
        # ====================================================
        eps_p_start = eps_p.x.array.copy()
        eqp_start = eqp.x.array.copy()
        n_cells_local = eps_p.x.array.size // 9
        eps_p_start_3d = eps_p_start.reshape((n_cells_local, 3, 3))

        eigenstrain = eps_th + eps_p
        L_form = ufl.inner(sigma_elastic(eigenstrain), eps(v)) * dx

        problem = LinearProblem(
            a, L_form, u=u, bcs=bcs,
            petsc_options_prefix="mechanics_",
            petsc_options={
                "ksp_type": "preonly",
                "pc_type": "lu",
                "pc_factor_mat_solver_type": "mumps"
            },
        )

        converged = False
        final_plastic_error = np.inf
        final_d_eps_p = None
        final_d_eqp = None

        for inner_it in range(plastic_inner_max_it):
            eps_p_guess = eps_p.x.array.copy()

            problem.solve()
            u.x.scatter_forward()

            eps_total = eps(u)
            eps_elastic_trial_expr = eps_total - eps_th - eps_p
            sigma_trial_expr = sigma_elastic(eps_elastic_trial_expr)
            
            sigma_trial = fem.Function(Qp_tensor)
            sigma_trial.interpolate(fem.Expression(sigma_trial_expr, Qp_tensor.element.interpolation_points))
            sigma_trial.x.scatter_forward()

            sy_expr = yield_strength(T_curr)
            sy = fem.Function(Qp_scalar)
            sy.interpolate(fem.Expression(sy_expr, Qp_scalar.element.interpolation_points))
            sy.x.scatter_forward()

            mu_expr = youngs_modulus(T_curr) / (2.0 * (1.0 + poisson_ratio(T_curr)))
            mu_dg = fem.Function(Qp_scalar)
            mu_dg.interpolate(fem.Expression(mu_expr, Qp_scalar.element.interpolation_points))
            mu_dg.x.scatter_forward()

            H_expr = hardening_modulus(T_curr, H0=H0, H_min_factor=H_min_factor, T_ref=H_T_ref, T_end=H_T_end)
            H_dg = fem.Function(Qp_scalar)
            H_dg.interpolate(fem.Expression(H_expr, Qp_scalar.element.interpolation_points))
            H_dg.x.scatter_forward()

            sigma_trial_np = sigma_trial.x.array.reshape((n_cells_local, 3, 3))
            
            # Apply Gum Phase factor to mechanical material parameters
            sy0_np = sy.x.array.copy() * gum_factor_np
            mu_np = mu_dg.x.array.copy() * gum_factor_np
            H_np = H_dg.x.array.copy() * gum_factor_np

            eqp_start_np = eqp_start.copy()
            sigma_y_np = sy0_np + H_np * eqp_start_np

            d_eps_p_step, d_eqp_step, _ = j2_radial_return(sigma_trial_np, sigma_y_np, mu_np, H_np)

            eps_p_target = eps_p_start_3d + d_eps_p_step
            eps_p_relaxed = (1.0 - plastic_relaxation) * eps_p_guess.reshape((n_cells_local, 3, 3)) + plastic_relaxation * eps_p_target
            eps_p.x.array[:] = eps_p_relaxed.reshape(-1)
            eps_p.x.scatter_forward()

            numerator = np.linalg.norm(eps_p.x.array - eps_p_guess)
            denominator = max(np.linalg.norm(eps_p.x.array), 1.0e-16)
            plastic_error = numerator / denominator

            final_plastic_error = plastic_error
            final_d_eps_p = d_eps_p_step
            final_d_eqp = d_eqp_step

            if comm.rank == 0:
                print(f"  inner {inner_it + 1:02d}: plastic error = {plastic_error:.4e}")

            if plastic_error < plastic_inner_tol:
                converged = True
                break

        if converged:
            if comm.rank == 0:
                print(f"Plastic iteration converged in {inner_it + 1} iterations.")
        else:
            if comm.rank == 0:
                print("WARNING: plastic iteration did not reach tolerance.")

        if comm.rank == 0:
            print(f"  final plastic error = {final_plastic_error:.6e}")

        if final_d_eps_p is not None:
            eps_p.x.array[:] = (eps_p_start_3d + final_d_eps_p).reshape(-1)
            eps_p.x.scatter_forward()

        if final_d_eqp is not None:
            eqp.x.array[:] = eqp_start + final_d_eqp
            eqp.x.scatter_forward()

        problem.solve()
        u.x.scatter_forward()
        T_prev.x.array[:] = T_curr.x.array
        T_prev.x.scatter_forward()

        # ====================================================
        # Post-processing
        # ====================================================
        sigma_tensor, vm = compute_stress_fields(mesh, u, T_curr, eps_p, eps_th)

        output_freq = int(getattr(cfg, "output_every", 1))
        
        if (step % output_freq == 0 or step == n_steps - 1):
            writer.write_function(u, t_val)
            writer.write_function(T_curr, t_val)
            writer.write_function(eqp, t_val)
            writer.write_function(eps_p, t_val)
            writer.write_function(sigma_tensor, t_val)
            writer.write_function(vm, t_val)
            writer.write_function(activated_mech, t_val)

        # اضافه کردن داده‌های این گام به بافر به صورت مستقیم
        results_buffer.append({
            "time": t_val,
            "displacement_3d": u.x.array.reshape((-1, 3)).tolist(),
            "equivalent_plastic_strain": eqp.x.array.tolist(),
            "temperatures": T_curr.x.array.tolist(),
            "von_mises_stress": vm.x.array.tolist(),
            "material_state": activated_mech.x.array.tolist()
        })

        if comm.rank == 0:
            print(f"  max |u| = {np.max(np.abs(u.x.array)):.6e}")
            print(f"  max eqp = {np.max(eqp.x.array):.6e}")
            print(f"  max von Mises = {np.max(vm.x.array):.6e} Pa")

    writer.close()
    
    # نوشتن کل بافر روی دیسک به صورت یکجا در انتهای شبیه‌سازی
    with open(os.path.join(out_dir, "mechanical_results.jsonl"), "w") as f:
        for record in results_buffer:
            f.write(json.dumps(record) + "\n")

    if comm.rank == 0:
        print("\n======================================================================")
        print("Mechanical analysis completed.")
        print("======================================================================")

    return u, eps_p, eqp, eps_th