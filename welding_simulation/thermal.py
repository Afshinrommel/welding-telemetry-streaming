import numpy as np
import ufl
from mpi4py import MPI
from petsc4py import PETSc

from dolfinx import fem, io
from dolfinx.fem.petsc import LinearProblem

from material import conductivity, heat_capacity
from heat_source import goldak_source


def boundary_facets(mesh):
    """Mark all exterior facets as boundary 1."""
    tdim = mesh.topology.dim
    fdim = tdim - 1
    mesh.topology.create_connectivity(fdim, tdim)

    from dolfinx.mesh import exterior_facet_indices
    facets = exterior_facet_indices(mesh.topology)

    values = np.full(len(facets), 1, dtype=np.int32)
    return facets, values


def solve_thermal(cfg, mesh, out_dir, cell_tags=None, exporter=None):
    """
    Sequential transient heat solve with Gum Phase (Dummy Elements) support:
        rho*cp*(T-T_old)/dt - div(k grad T) = q
    with convection + radiation on all exterior surfaces.
    """

    Q = fem.functionspace(mesh, ("Lagrange", 1))
    
    # DG0 space for element-wise (cell-based) Gum Phase tracking
    Q_mat = fem.functionspace(mesh, ("DG", 0))
    is_bead = fem.Function(Q_mat)
    activated = fem.Function(Q_mat)
    activated.name = "MaterialState"  # نام‌‌گذاری صریح برای ParaView
    activated.x.array[:] = 1.0
    is_bead.x.array[:] = 0.0

    if cell_tags is not None:
        bead_cells = cell_tags.find(1)
        if len(bead_cells) > 0:
            is_bead.x.array[bead_cells] = 1.0
            activated.x.array[bead_cells] = 0.0  # فاز آدامسی اولیه برای فیلر
            print(f"-> Thermal Gum Phase: {len(bead_cells)} weld bead cells set to inactive.")
        else:
            print("-> WARNING: Tag 2 (WeldBead) was not found in cell_tags!")

    T = fem.Function(Q, name="Temperature")
    T_old = fem.Function(Q, name="Temperature")

    T.x.array[:] = cfg.T0
    T_old.x.array[:] = cfg.T0

    # Boundary measure
    facets, values = boundary_facets(mesh)
    fdim = mesh.topology.dim - 1
    from dolfinx.mesh import meshtags
    facet_tags = meshtags(
        mesh,
        fdim,
        facets,
        values,
    )

    dx = ufl.Measure("dx", domain=mesh)
    ds = ufl.Measure(
        "ds",
        domain=mesh,
        subdomain_data=facet_tags,
    )

    v = ufl.TestFunction(Q)
    u = ufl.TrialFunction(Q)

    x = ufl.SpatialCoordinate(mesh)

    K = fem.Function(Q_mat, name="Conductivity")
    CP = fem.Function(Q_mat, name="HeatCapacity")

    def update_material_fields(Tfun):
        Tarr = Tfun.x.array.real
        
        # Interpolate temperature to DG0 for cell-based gum phase evaluation
        T_dg_expr = fem.Expression(Tfun, Q_mat.element.interpolation_points)
        T_dg = fem.Function(Q_mat)
        T_dg.interpolate(T_dg_expr)
        T_cell = T_dg.x.array

        if cell_tags is not None:
            # Dynamic activation of filler elements when reaching activation threshold (500 C)
            activation_mask = (is_bead.x.array > 0.5) & (T_cell >= 500.0) & (activated.x.array < 0.5)
            if np.any(activation_mask):
                activated.x.array[activation_mask] = 1.0
                activated.x.scatter_forward()
            
            gum_factors = np.where(is_bead.x.array > 0.5, np.where(activated.x.array > 0.5, 1.0, 1e-4), 1.0)
        else:
            gum_factors = 1.0

        k_vals = conductivity(T_cell) * gum_factors
        cp_vals = heat_capacity(T_cell)
        
        K.x.array[:] = k_vals
        CP.x.array[:] = cp_vals
        K.x.scatter_forward()
        CP.x.scatter_forward()

    update_material_fields(T_old)

    rho = fem.Constant(mesh, PETSc.ScalarType(cfg.rho))
    dt_c = fem.Constant(mesh, PETSc.ScalarType(cfg.dt))
    T_ambient = fem.Constant(mesh, PETSc.ScalarType(cfg.Tamb))
    h = fem.Constant(mesh, PETSc.ScalarType(cfg.h_conv))
    eps = fem.Constant(mesh, PETSc.ScalarType(cfg.emissivity))
    sigma = fem.Constant(mesh, PETSc.ScalarType(cfg.sigma_sb))

    Tprev = T_old
    Tprev_K = Tprev + 273.15
    Tamb_K = cfg.Tamb + 273.15
    qrad = eps * sigma * (Tprev_K**4 - Tamb_K**4)
    qconv = h * (Tprev - T_ambient)

    time_constant = fem.Constant(mesh, PETSc.ScalarType(0.0))
    q = goldak_source(x, time_constant, cfg)

    a = (
        rho * CP / dt_c * u * v * dx
        + K * ufl.dot(ufl.grad(u), ufl.grad(v)) * dx
    )

    L = (
        rho * CP / dt_c * T_old * v * dx
        + q * v * dx
        - (qconv + qrad) * v * ds(1)
    )

    problem = LinearProblem(
        a,
        L,
        petsc_options_prefix="thermal_",
        petsc_options={
            "ksp_type": "cg",
            "pc_type": "hypre",
            "pc_hypre_type": "boomeramg",
            "ksp_monitor": "",
            "ksp_converged_reason": "",
        },
        u=T,
    )

    xdmf = None
    if mesh.comm.rank == 0:
        out_dir.mkdir(parents=True, exist_ok=True)

    mesh.comm.barrier()

    xdmf = io.XDMFFile(
        mesh.comm,
        str(out_dir / "thermal.xdmf"),
        "w",
    )
    xdmf.write_mesh(mesh)

    thermal_history = []
    for step in range(1, cfg.nsteps + 1):
        t = step * cfg.dt
        time_constant.value = PETSc.ScalarType(t)

        update_material_fields(T_old)

        problem.solve()
        T.x.scatter_forward()

        if step % cfg.output_every == 0 or step == 1:
            xdmf.write_function(T, t)
            xdmf.write_function(activated, t)
            
            if exporter is not None:
                exporter.append_thermal_step(t, T, material_state_function=activated) # ارسال حالت ماده به فایل JSONL
            thermal_history.append((t, T.x.array.copy()))

        T_old.x.array[:] = T.x.array
        T_old.x.scatter_forward()

        if mesh.comm.rank == 0 and step % cfg.output_every == 0:
            print(f"[thermal] t={t:8.3f} s")

    xdmf.close()

    if exporter is not None:
        exporter.dump_thermal_history()

    return thermal_history, T

def build_thermal_mesh(cfg):
    """
    Fallback structured mesh generator if needed.
    """
    import numpy as np
    from dolfinx import mesh
    from mpi4py import MPI
    
    L = cfg.length
    W = cfg.width
    T = cfg.thickness
    
    nx = getattr(cfg, 'nx_mesh', 40)
    ny_half = getattr(cfg, 'ny_half', 12)
    nz = getattr(cfg, 'nz_mesh', 3)
    ny_total = ny_half * 2
    
    domain = mesh.create_box(
        MPI.COMM_WORLD,
        [np.array([0.0, 0.0, 0.0]), np.array([L, W, T])],
        [nx, ny_total, nz],
        cell_type=mesh.CellType.hexahedron
    )
    return domain