import numpy as np
import ufl

def _is_numeric(val):
    """If the input is a standard array or number, return True. Otherwise (like FEniCSx function), return False."""
    return isinstance(val, (int, float, list, tuple, np.ndarray, np.generic))

def conductivity(T, gum_factor=1.0):
    if not _is_numeric(T):
        k = ufl.conditional(ufl.lt(T, 800.0), 54.0 - 0.0333 * T, 27.3)
        k_eff = ufl.conditional(k >= 15.0, k, 15.0)
        return k_eff * gum_factor
    T = np.asarray(T, dtype=float)
    k = np.where(T < 800.0, 54.0 - 0.0333 * T, 27.3)
    return np.maximum(k, 15.0) * gum_factor

def heat_capacity(T):
    if not _is_numeric(T):
        return 650.0
    T = np.asarray(T, dtype=float)
    cp = np.full_like(T, 650.0)
    m1 = T < 600.0
    cp[m1] = 425.0 + 0.773 * T[m1] - 1.69e-3 * T[m1] ** 2 + 2.22e-6 * T[m1] ** 3
    m2 = (T >= 600.0) & (T < 735.0)
    cp[m2] = 666.0 + 13002.0 / np.maximum(738.0 - T[m2], 1.0)
    m3 = (T >= 735.0) & (T < 900.0)
    cp[m3] = 545.0 + 17820.0 / np.maximum(T[m3] - 731.0, 1.0)
    return np.clip(cp, 400.0, 5000.0)

def youngs_modulus(T, gum_factor=1.0):
    if not _is_numeric(T):
        E = ufl.conditional(ufl.lt(T, 100.0), 200e9, 200e9 * (1.0 - (T - 100.0) / 1000.0))
        E_eff = ufl.conditional(E >= 10e9, E, 10e9)
        return E_eff * gum_factor
    T = np.asarray(T, dtype=float)
    E = np.where(T < 100.0, 200e9, 200e9 * (1.0 - (T - 100.0) / 1000.0))
    return np.maximum(E, 10e9) * gum_factor

def poisson_ratio(T):
    if not _is_numeric(T):
        return 0.30
    return np.full_like(np.asarray(T, dtype=float), 0.30)

def yield_strength(T, gum_factor=1.0):
    if not _is_numeric(T):
        sy = ufl.conditional(ufl.lt(T, 400.0), 250e6, 250e6 * (1.0 - (T - 400.0) / 700.0))
        sy_eff = ufl.conditional(sy >= 5e6, sy, 5e6)
        return sy_eff * gum_factor
    T = np.asarray(T, dtype=float)
    sy = np.where(T < 400.0, 250e6, 250e6 * (1.0 - (T - 400.0) / 700.0))
    return np.maximum(sy, 5e6) * gum_factor

def expansion_coefficient(T):
    if not _is_numeric(T):
        return ufl.conditional(ufl.lt(T, 800.0), 1.4e-5, 1.6e-5)
    T = np.asarray(T, dtype=float)
    return np.where(T < 800.0, 1.4e-5, 1.6e-5)

def hardening_modulus(T, H0=1.0e9, H_min_factor=0.10, T_ref=400.0, T_end=1100.0, gum_factor=1.0):
    if not _is_numeric(T):
        factor = ufl.conditional(
            ufl.le(T, T_ref),
            1.0,
            ufl.conditional(
                ufl.ge(T, T_end),
                H_min_factor,
                1.0 - (1.0 - H_min_factor) * (T - T_ref) / (T_end - T_ref)
            )
        )
        return H0 * factor * gum_factor
    T = np.asarray(T, dtype=float)
    factor = np.ones_like(T)
    middle = (T > T_ref) & (T < T_end)
    factor[middle] = 1.0 - (1.0 - H_min_factor) * (T[middle] - T_ref) / (T_end - T_ref)
    factor[T >= T_end] = H_min_factor
    return H0 * factor * gum_factor