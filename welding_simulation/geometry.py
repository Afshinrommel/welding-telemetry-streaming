import gmsh
import math
import meshio
import numpy as np


# ============================================================
# CONFIGURATION
# ============================================================

class Config:

    # --------------------------------------------------------
    # Plate dimensions [m]
    # --------------------------------------------------------

    L = 0.120
    W = 0.100
    H = 0.003

    # --------------------------------------------------------
    # Weld width [m]
    # --------------------------------------------------------

    weld_width = 0.0015

    # --------------------------------------------------------
    # X mesh size [m]
    # Constant over entire plate
    # --------------------------------------------------------

    x_mesh_size = 0.002

    # --------------------------------------------------------
    # Y mesh sizes [m]
    # --------------------------------------------------------

    weld_mesh_size = 0.0005
    near_mesh_size = 0.0010
    middle_mesh_size = 0.0020
    far_mesh_size = 0.0035

    # --------------------------------------------------------
    # Width of refinement zones on each side [m]
    # --------------------------------------------------------

    near_width = 0.003
    middle_width = 0.007

    # --------------------------------------------------------
    # Z mesh
    # --------------------------------------------------------

    nz = 6

    # --------------------------------------------------------
    # Output files
    # --------------------------------------------------------

    msh_file = "weld_mesh_final.msh"
    vtk_file = "weld_mesh_final.vtk"


cfg = Config()


# ============================================================
# SIMULATION PHASE DEFINITION
#
# IMPORTANT:
#
# Phase 0 = Base Metal
# Phase 1 = Weld / Gum
# ============================================================

PHASE_BASE = 0.0
PHASE_WELD = 1.0


# ============================================================
# GMSH PHYSICAL TAGS
#
# These numbers are ONLY Gmsh identifiers.
# They are NOT simulation phases.
# ============================================================

GMSH_BASE_TAG = 100
GMSH_WELD_TAG = 101


# ============================================================
# INITIALIZE GMSH
# ============================================================

gmsh.initialize()

gmsh.option.setNumber(
    "General.Terminal",
    1
)

gmsh.model.add(
    "WeldingStructuredHexMesh"
)

geo = gmsh.model.geo


# ============================================================
# MESH COUNTS
# ============================================================

nx = int(
    round(
        cfg.L / cfg.x_mesh_size
    )
)

nz = cfg.nz


# ============================================================
# Y COORDINATES
# ============================================================

y_center = cfg.W / 2.0

half_weld = cfg.weld_width / 2.0


y0 = 0.0

y1 = (
    y_center
    - half_weld
    - cfg.near_width
    - cfg.middle_width
)

y2 = (
    y_center
    - half_weld
    - cfg.near_width
)

y3 = (
    y_center
    - half_weld
)

y4 = (
    y_center
    + half_weld
)

y5 = (
    y_center
    + half_weld
    + cfg.near_width
)

y6 = (
    y_center
    + half_weld
    + cfg.near_width
    + cfg.middle_width
)

y7 = cfg.W


# ============================================================
# ZONE DEFINITIONS
# ============================================================

zones = [
    {
        "name": "Far_Left",
        "ymin": y0,
        "ymax": y1,
        "mesh_size": cfg.far_mesh_size
    },
    {
        "name": "Middle_Left",
        "ymin": y1,
        "ymax": y2,
        "mesh_size": cfg.middle_mesh_size
    },
    {
        "name": "Near_Left",
        "ymin": y2,
        "ymax": y3,
        "mesh_size": cfg.near_mesh_size
    },
    {
        "name": "Weld",
        "ymin": y3,
        "ymax": y4,
        "mesh_size": cfg.weld_mesh_size
    },
    {
        "name": "Near_Right",
        "ymin": y4,
        "ymax": y5,
        "mesh_size": cfg.near_mesh_size
    },
    {
        "name": "Middle_Right",
        "ymin": y5,
        "ymax": y6,
        "mesh_size": cfg.middle_mesh_size
    },
    {
        "name": "Far_Right",
        "ymin": y6,
        "ymax": y7,
        "mesh_size": cfg.far_mesh_size
    }
]


# ============================================================
# POINT CACHE
# ============================================================

point_cache = {}

def get_point(x, y, z):
    key = (round(x, 12), round(y, 12), round(z, 12))
    if key not in point_cache:
        point_cache[key] = geo.addPoint(x, y, z, 0.0)
    return point_cache[key]


# ============================================================
# CURVE CACHE
# ============================================================

curve_cache = {}

def get_curve(p1, p2):
    key = tuple(sorted((p1, p2)))
    if key not in curve_cache:
        curve_cache[key] = geo.addLine(p1, p2)
    tag = curve_cache[key]
    
    if p1 < p2:
        return tag
    else:
        return -tag


# ============================================================
# SURFACE CACHE
# ============================================================

surface_cache = {}

def get_surface(curves):
    key = tuple(sorted(abs(c) for c in curves))
    if key not in surface_cache:
        loop = geo.addCurveLoop(curves)
        surface_cache[key] = geo.addPlaneSurface([loop])
    return surface_cache[key]


# ============================================================
# VOLUME DATA
# ============================================================

volume_data = []


# ============================================================
# CREATE GEOMETRY
# ============================================================

for zone in zones:
    ymin = zone["ymin"]
    ymax = zone["ymax"]

    # Bottom
    p000 = get_point(0.0, ymin, 0.0)
    p100 = get_point(cfg.L, ymin, 0.0)
    p010 = get_point(0.0, ymax, 0.0)
    p110 = get_point(cfg.L, ymax, 0.0)

    # Top
    p001 = get_point(0.0, ymin, cfg.H)
    p101 = get_point(cfg.L, ymin, cfg.H)
    p011 = get_point(0.0, ymax, cfg.H)
    p111 = get_point(cfg.L, ymax, cfg.H)

    # Curves
    c_x0 = get_curve(p000, p100)
    c_y0 = get_curve(p000, p010)
    c_x1 = get_curve(p010, p110)
    c_y1 = get_curve(p100, p110)

    c_x0_top = get_curve(p001, p101)
    c_y0_top = get_curve(p001, p011)
    c_x1_top = get_curve(p011, p111)
    c_y1_top = get_curve(p101, p111)

    c_z00 = get_curve(p000, p001)
    c_z10 = get_curve(p100, p101)
    c_z01 = get_curve(p010, p011)
    c_z11 = get_curve(p110, p111)

    # Surfaces
    s_bottom = get_surface([c_x0, c_y1, -c_x1, -c_y0])
    s_top = get_surface([c_x0_top, c_y1_top, -c_x1_top, -c_y0_top])
    s_ymin = get_surface([c_x0, c_z10, -c_x0_top, -c_z00])
    s_ymax = get_surface([c_x1, c_z11, -c_x1_top, -c_z01])
    s_xmin = get_surface([c_y0, c_z01, -c_y0_top, -c_z00])
    s_xmax = get_surface([c_y1, c_z11, -c_y1_top, -c_z10])

    surface_loop = geo.addSurfaceLoop([s_bottom, s_top, s_ymin, s_ymax, s_xmin, s_xmax])
    volume = geo.addVolume([surface_loop])

    volume_data.append({
        "volume": volume,
        "zone": zone["name"],
        "ymin": ymin,
        "ymax": ymax
    })

geo.synchronize()


# ============================================================
# TRANSFINITE MESH
# ============================================================

for data in volume_data:
    volume = data["volume"]
    ymin = data["ymin"]
    ymax = data["ymax"]

    zone = None
    for z in zones:
        if (math.isclose(ymin, z["ymin"], abs_tol=1e-12) and 
            math.isclose(ymax, z["ymax"], abs_tol=1e-12)):
            zone = z
            break

    if zone is None:
        raise RuntimeError("Mesh zone not found.")

    dy = ymax - ymin
    ny = max(1, int(round(dy / zone["mesh_size"])))

    boundary_surfaces = gmsh.model.getBoundary([(3, volume)], oriented=False, recursive=False)
    surface_tags = [tag for dim, tag in boundary_surfaces if dim == 2]

    curve_tags = set()
    for surface in surface_tags:
        boundary_curves = gmsh.model.getBoundary([(2, surface)], oriented=False, recursive=False)
        for dim, tag in boundary_curves:
            if dim == 1:
                curve_tags.add(tag)

    for curve in curve_tags:
        points = gmsh.model.getBoundary([(1, curve)], oriented=False, recursive=False)
        point_tags = [tag for dim, tag in points if dim == 0]

        if len(point_tags) != 2:
            continue

        xyz = [gmsh.model.getValue(0, p, []) for p in point_tags]
        p1 = xyz[0]
        p2 = xyz[1]

        dx = abs(p2[0] - p1[0])
        dy_curve = abs(p2[1] - p1[1])
        dz = abs(p2[2] - p1[2])

        if (dx > 1e-12 and dy_curve < 1e-12 and dz < 1e-12):
            gmsh.model.mesh.setTransfiniteCurve(curve, nx + 1)
        elif (dy_curve > 1e-12 and dx < 1e-12 and dz < 1e-12):
            gmsh.model.mesh.setTransfiniteCurve(curve, ny + 1)
        elif (dz > 1e-12 and dx < 1e-12 and dy_curve < 1e-12):
            gmsh.model.mesh.setTransfiniteCurve(curve, nz + 1)

    for surface in surface_tags:
        try:
            gmsh.model.mesh.setTransfiniteSurface(surface)
        except Exception:
            pass

    gmsh.model.mesh.setTransfiniteVolume(volume)


for dim, tag in gmsh.model.getEntities(2):
    try:
        gmsh.model.mesh.setRecombine(2, tag)
    except Exception:
        pass


# ============================================================
# PHYSICAL GROUPS
# ============================================================

weld_volumes = []
base_volumes = []

for data in volume_data:
    if data["zone"] == "Weld":
        weld_volumes.append(data["volume"])
    else:
        base_volumes.append(data["volume"])

gmsh.model.addPhysicalGroup(3, base_volumes, GMSH_BASE_TAG)
gmsh.model.setPhysicalName(3, GMSH_BASE_TAG, "BaseMetal")

gmsh.model.addPhysicalGroup(3, weld_volumes, GMSH_WELD_TAG)
gmsh.model.setPhysicalName(3, GMSH_WELD_TAG, "Weld_Gum_Phase")


print("\nGenerating 3D mesh...")
gmsh.model.mesh.generate(3)
gmsh.write(cfg.msh_file)
gmsh.finalize()


# ============================================================
# MESHIO PROCESSING
# ============================================================

print("Creating explicit Phase field...")
mesh = meshio.read(cfg.msh_file)
physical_data = mesh.cell_data.get("gmsh:physical", None)
phase_data = []

for i, cell_block in enumerate(mesh.cells):
    cell_type = cell_block.type
    n_cells = len(cell_block.data)
    phase = np.zeros(n_cells, dtype=np.float64)

    if cell_type in ("hexahedron", "hexahedron27"):
        if physical_data is not None:
            physical_tags = physical_data[i]
            phase[physical_tags == GMSH_WELD_TAG] = PHASE_WELD
            phase[physical_tags == GMSH_BASE_TAG] = PHASE_BASE
    
    phase_data.append(phase)

mesh.cell_data["Phase"] = phase_data

meshio.write(cfg.msh_file, mesh, file_format="gmsh")
meshio.write(cfg.vtk_file, mesh, file_format="vtk")

# ============================================================
# SAVE XDMF FOR FENICSX (Merged for FEniCSx Strict Topology)
# ============================================================
print("Merging mesh blocks for FEniCSx (Strict Hexahedron Topology)...")

hex_cells_data = []
hex_phase_data = []

for i, cell_block in enumerate(mesh.cells):
    if cell_block.type == "hexahedron":
        hex_cells_data.append(cell_block.data)
        hex_phase_data.append(mesh.cell_data["Phase"][i])

if not hex_cells_data:
    raise RuntimeError("هیچ المان مکعبی در مش یافت نشد!")

# چسباندن تمام بلوک‌های مکعبی و آرایه‌های فاز به یک بلوک و آرایه واحد
combined_hex_cells = np.vstack(hex_cells_data)
combined_hex_phase = np.concatenate(hex_phase_data)

# ساخت مش یکپارچه سه‌بعدی خالص
pure_3d_mesh = meshio.Mesh(
    points=mesh.points,
    cells=[("hexahedron", combined_hex_cells)],
    cell_data={"Phase": [combined_hex_phase]}
)

xdmf_file = "weld_mesh_final.xdmf"
meshio.write(xdmf_file, pure_3d_mesh)
print(f"  Clean & Merged XDMF saved for FEniCSx: {xdmf_file}")


# ============================================================
# FENICSX INTEGRATION
# ============================================================
import os

def build_welded_mesh(comm, cfg):
    """
    این تابع توسط main.py فراخوانی می‌شود تا مش تولید شده را 
    در حل‌گر FEniCSx بارگذاری کند.
    """
    from dolfinx.io import XDMFFile
    
    # استفاده از نام مستقیم فایل برای جلوگیری از خطای AttributeError
    mesh_file = "weld_mesh_final.xdmf"
    
    if not os.path.exists(mesh_file):
        if comm.rank == 0:
            print(f"Mesh file '{mesh_file}' not found! Please run geometry.py standalone first.")
        return None, None

    # بارگذاری مش یکپارچه در حل‌گر
    with XDMFFile(comm, mesh_file, "r") as xdmf:
        domain = xdmf.read_mesh(name="Grid")
        
        # خواندن فیلد Phase به عنوان تگ‌های سلولی
        try:
            cell_tags = xdmf.read_meshtags(domain, name="Grid")
        except:
            cell_tags = xdmf.read_meshtags(domain, name="Phase")
            
    return domain, cell_tags