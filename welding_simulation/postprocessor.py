import ufl
from dolfinx import fem
from material import youngs_modulus, poisson_ratio

def eps(u):
    """Small-strain tensor."""
    return ufl.sym(ufl.grad(u))

def dev(A):
    """Deviatoric part of a 3x3 tensor."""
    return A - ufl.tr(A) / 3.0 * ufl.Identity(3)

def compute_stress_fields(mesh, displacement, temperature, plastic_strain, thermal_strain):
    """
    Computes both the full Cauchy Stress Tensor and the Von Mises scalar field.
    """
    DG0_scalar = fem.functionspace(mesh, ("DG", 0))
    DG0_tensor = fem.functionspace(mesh, ("DG", 0, (3, 3)))

    E = youngs_modulus(temperature)
    nu = poisson_ratio(temperature)

    mu = E / (2.0 * (1.0 + nu))
    lam = E * nu / ((1.0 + nu) * (1.0 - 2.0 * nu))

    eps_elastic = eps(displacement) - thermal_strain - plastic_strain

    # 1. Full Stress Tensor
    sigma = lam * ufl.tr(eps_elastic) * ufl.Identity(3) + 2.0 * mu * eps_elastic
    
    sigma_func = fem.Function(DG0_tensor, name="StressTensor")
    sigma_func.interpolate(
        fem.Expression(sigma, DG0_tensor.element.interpolation_points)
    )
    sigma_func.x.scatter_forward()

    # 2. Von Mises Stress
    s = dev(sigma)
    vm_expr = ufl.sqrt(ufl.max_value(1.0e-24, 1.5 * ufl.inner(s, s)))
    
    vm_func = fem.Function(DG0_scalar, name="VonMisesStress")
    vm_func.interpolate(
        fem.Expression(vm_expr, DG0_scalar.element.interpolation_points)
    )
    vm_func.x.scatter_forward()

    return sigma_func, vm_func