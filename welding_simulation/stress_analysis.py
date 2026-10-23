import ufl
from dolfinx import fem, io
from material import youngs_modulus, poisson_ratio, expansion_coefficient

def save_stress_components(cfg, mesh, temperature, displacement, out_dir):
    """
    Computes the full 3x3 stress tensor and saves it to a separate XDMF file.
    """
    # ایجاد یک فضای تابعی تانسوری ۳ در ۳ در المان‌ها (DG0)
    T_space = fem.functionspace(mesh, ("Discontinuous Lagrange", 0, (3, 3)))
    stress_tensor = fem.Function(T_space, name="StressTensor")

    # فضاهای اسکالر برای درون‌یابی خواص ماده
    Q = fem.functionspace(mesh, ("Lagrange", 1))
    E = fem.Function(Q)
    nu = fem.Function(Q)
    alpha = fem.Function(Q)
    
    E.x.array[:] = youngs_modulus(temperature.x.array.real)
    nu.x.array[:] = poisson_ratio(temperature.x.array.real)
    alpha.x.array[:] = expansion_coefficient(temperature.x.array.real)

    # معادلات کرنش و تنش ترموالاستیک
    eps = ufl.sym(ufl.grad(displacement))
    I = ufl.Identity(3)

    mu = E / (2.0 * (1.0 + nu))
    lam = E * nu / ((1.0 + nu) * (1.0 - 2.0 * nu))

    eth = alpha * (temperature - cfg.T0)

    # محاسبه ماتریس کامل تنش (تانسور ۳x۳)
    sigma = (
        lam * ufl.tr(eps - eth * I) * I
        + 2.0 * mu * (eps - eth * I)
    )

    # جای‌گذاری مقادیر تانسور در فضای تابعی تعریف‌شده
    expr = fem.Expression(sigma, T_space.element.interpolation_points)
    stress_tensor.interpolate(expr)

    # ذخیره‌سازی نتیجه در یک فایل مجزا
    if mesh.comm.rank == 0:
        out_dir.mkdir(parents=True, exist_ok=True)
    mesh.comm.barrier()

    with io.XDMFFile(mesh.comm, str(out_dir / "stress_components.xdmf"), "w") as xdmf:
        xdmf.write_mesh(mesh)
        xdmf.write_function(stress_tensor, 1.0)
    
    return stress_tensor
