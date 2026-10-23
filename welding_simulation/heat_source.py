import ufl
import numpy as np

class DoubleEllipsoidHeatSource:
    def __init__(self, Q, ff, fr, a_front, a_rear, b, c, v):
        self.Q = Q
        self.ff = ff
        self.fr = fr
        self.a_front = a_front
        self.a_rear = a_rear
        self.b = b
        self.c = c
        self.v = v
        
    def __call__(self, x, y, z, t):
        pass

def goldak_source(x, t, cfg):
    """
    محاسبه شار حرارتی گلدک (Goldak's Double Ellipsoid Heat Source) سازگار با UFL
    """
    Q = cfg.power_W * cfg.efficiency
    ff = cfg.ff
    fr = cfg.fr
    a_f = cfg.a_front
    a_r = cfg.a_rear
    b = cfg.b
    c = cfg.c
    v = cfg.speed_m_s

    # مرکز منبع حرارتی در حال حرکت در امتداد محور x
    x0 = v * t
    y0 = cfg.width / 2.0
    z0 = cfg.thickness

    dx = x[0] - x0
    dy = x[1] - y0
    dz = x[2] - z0

    # تفکیک بخش جلو و عقب منبع حرارتی با استفاده از دستورات شرطی UFL
    is_front = ufl.ge(dx, 0.0)
    
    a_val = ufl.conditional(is_front, a_f, a_r)
    f_val = ufl.conditional(is_front, ff, fr)

    # فرمول توزیع شار حرارتی گلدک
    pi = np.pi
    factor = (6.0 * np.sqrt(3.0) * f_val * Q) / (pi * ufl.sqrt(pi) * a_val * b * c)
    exponent = -3.0 * ((dx ** 2) / (a_val ** 2) + (dy ** 2) / (b ** 2) + (dz ** 2) / (c ** 2))
    
    q = factor * ufl.exp(exponent)
    
    return q
