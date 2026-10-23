from dataclasses import dataclass

@dataclass
class Config:
    # Geometry [m]
    length: float = 0.120
    width: float = 0.10
    thickness: float = 0.003
    bead_width: float = 0.004   
    bead_height: float = 0.0015 

    # Mesh divisions (Structured with Biasing)
    nx_mesh: int = 40
    ny_half: int = 12
    nz_mesh: int = 3
    bias_y: float = 1.35

    # Initial/environment temperature [C]
    T0: float = 25.0
    Tamb: float = 25.0

    # Welding
    power_W: float = 1800.0
    efficiency: float = 0.75
    speed_m_s: float = 0.005

    # Goldak parameters [m]
    a_front: float = 0.004
    a_rear: float = 0.008
    b: float = 0.004
    c: float = 0.002

    # Goldak fractions; must sum to 2.0
    ff: float = 0.6
    fr: float = 1.4

    # Thermal boundary parameters
    h_conv: float = 15.0          # W/(m2 K)
    emissivity: float = 0.70
    sigma_sb: float = 5.670374419e-8

    # Material
    rho: float = 7850.0           # kg/m3
    alpha0: float = 1.4e-5        # 1/K

    # Time
    total_time: float = 4
    dt: float = 0.02
    output_every: int = 10

    # Mechanical boundary condition:
    # clamp the x=0 end in all displacement components.
    # This is a modelling placeholder and must be replaced by
    # the actual fixture/contact conditions for engineering use.

    @property
    def absorbed_power(self):
        return self.power_W * self.efficiency

    @property
    def nsteps(self):
        return int(round(self.total_time / self.dt))