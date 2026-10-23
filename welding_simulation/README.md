3D Thermo-Mechanical Welding Simulation Project with FEniCSx

This project provides a complete pipeline for simulating the thermal and elastoplastic mechanical behavior of a 3D welding process. It is designed to run on Google Colab using the FEniCSx finite element solver and the Gmsh meshing library.

Physical Model & Boundary Conditions

To ensure high-fidelity physical realism, this simulation incorporates non-linear temperature-dependent material properties, a moving heat source, and dynamic boundary conditions:

Geometry & Mesh: A 120 × 100 × 3 mm steel plate modeled with a structured HEX8 finite element mesh.

Heat Source: Moving heat flux based on Goldak's Double-Ellipsoid model (Power: 1800W, Efficiency: 75%, Speed: 0.005 m/s).

Thermal Boundary Conditions: Surface heat loss computed dynamically via combined Convection (h = 15.0 W/m²K) and Radiation (Emissivity = 0.70) to an ambient temperature of 25°C.

Mechanical Model: Transient J2 von-Mises elastoplasticity with linear isotropic hardening.

Mechanical Boundary Conditions: The plate is fully clamped at the x=0 edge (all displacement components fixed).

Material Properties: Density is set to 7850 kg/m³. Thermal conductivity, heat capacity, Young's modulus, and yield strength are fully temperature-dependent.

Prerequisites and Environment

This simulation is built for the Google Colab environment. All dependencies, including Message Passing Interface (MPI) libraries and the FEniCSx finite element engine, are automatically downloaded and compiled via the provided execution scripts.

Key libraries used in this project include:

FEniCSx (dolfinx) for solving partial differential equations (PDEs).

mpi4py and PETSc for parallel processing and linear/nonlinear solvers.

Gmsh and Meshio for generating and converting finite element meshes (HEX8).

h5py and the XDMF data format for handling large-scale simulation outputs.

How to Run the Simulation (Colab Commands)

Execute the following commands step-by-step in your Google Colab notebook cells:

1. Mount Google Drive and Set Working Directory

from google.colab import drive
drive.mount('/content/drive')


%cd /content/drive/MyDrive/welding_simulation


2. Install System Dependencies (OpenGL and OpenMPI)

!apt-get update && apt-get install -y libglu1-mesa
!apt-get install -y libopenmpi-dev


3. Install FEniCSx Framework

!wget "[https://fem-on-colab.github.io/releases/fenicsx-install-release-real.sh](https://fem-on-colab.github.io/releases/fenicsx-install-release-real.sh)" -O "/tmp/fenicsx-install.sh" && bash "/tmp/fenicsx-install.sh"


4. Install Python Packages (MPI, Gmsh, Meshio)

!pip install mpi4py gmsh meshio


5. Generate Geometry and Mesh

!python3 geometry.py


6. Run the Main Thermo-Mechanical Simulation

!python main.py


7. Extract HEX8 Topology for Dashboard

!python extract_real_mesh.py


8. Compress and Download Results

!zip -r /content/results.zip /content/drive/MyDrive/welding_simulation/results
from google.colab import files
files.download('/content/results.zip')


Results and Outputs

Simulation outputs are saved in the results directory and include:

Standard .xdmf and .h5 file pairs for the thermal (thermal.xdmf) and mechanical (mechanical_elastoplastic.xdmf) phases.

Data streams in JSON Lines format (thermal_series.jsonl and mechanical_results.jsonl) optimized for data pipelines.

Mesh graph topology data (mesh_topology.json and volumetric_elements.jsonl).

Important Note Regarding Simulation Time (config.py)

Note: To prevent the generation of excessively large files during testing and demonstration, the total simulation time in config.py is currently set to 4 seconds (with a time step of 0.02 seconds).

However, the complete simulation covering a duration of 100 seconds has already been successfully executed. The full results of this 100-second simulation are available as two distinct, large-scale datasets hosted on Google Drive at the provided URLs.