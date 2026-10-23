from pathlib import Path
from mpi4py import MPI
import dolfinx
import numpy as np

from config import Config
from material import conductivity, heat_capacity, youngs_modulus, poisson_ratio, expansion_coefficient
from heat_source import DoubleEllipsoidHeatSource
from thermal import solve_thermal
from geometry import build_welded_mesh
from mechanical import solve_mechanics_transient
from dataset_exporter import FEniCSxJSONExporter

def run_simulation():
    comm = MPI.COMM_WORLD
    cfg = Config()
    out_dir = Path("results")

    if comm.rank == 0:
        print(f"--- Starting 3D Thermo-Mechanical Welding Simulation ---")
        print(f"Total time: {cfg.total_time} s, Time step (dt): {cfg.dt} s")

    # 1. Create Welded Mesh and Tags
    domain, cell_tags = build_welded_mesh(comm, cfg)

    # Initialize JSON Exporter
    exporter = FEniCSxJSONExporter(out_dir)
    exporter.export_topology(domain)

    # 2. Thermal Analysis (Transient)
    if comm.rank == 0:
        print("\n--- Running Transient Thermal Analysis ---")
    
    
    thermal_history, final_temperature = solve_thermal(cfg, domain, out_dir, cell_tags=cell_tags, exporter=exporter)
    exporter.dump_thermal_history()

    # 3. Mechanical Analysis (Transient J2 Elastoplastic)
    if comm.rank == 0:
        print("\n--- Running Transient J2 Elastoplastic Mechanical Analysis ---")
    
    displacement, plastic_strain, eq_plastic_strain, thermal_strain = solve_mechanics_transient(
    cfg, domain, thermal_history, out_dir, cell_tags=cell_tags, exporter=exporter)


    if comm.rank == 0:
        print("\nSimulation successfully completed and all outputs exported!")

if __name__ == "__main__":
    run_simulation()