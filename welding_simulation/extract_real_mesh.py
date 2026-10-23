import json
from mpi4py import MPI
from config import Config
from geometry import build_welded_mesh

def extract_exact_topology():
    comm = MPI.COMM_WORLD
    cfg = Config()
    
    print("Loading mesh from weld_mesh_final.xdmf...")
    domain, cell_tags = build_welded_mesh(comm, cfg)
    
    if domain is None:
        print("Error: Could not load the mesh. Make sure weld_mesh_final.xdmf exists in the directory.")
        return

    print("Mesh loaded successfully. Extracting HEX8 elements topology...")
    
    output_filename = "volumetric_elements.jsonl"
    elements = domain.geometry.dofmap.tolist()
    
    with open(output_filename, "w") as f:
        for idx, nodes in enumerate(elements):
            record = {
                "element_id": idx + 1,
                "type": "HEX8",
                "nodes": nodes
            }
            f.write(json.dumps(record) + "\n")
            
    print(f"Success: {len(elements)} real volumetric elements exported to '{output_filename}'.")
    print("Export completed. Copy this JSONL file to your Streamlit dashboard directory.")

if __name__ == "__main__":
    extract_exact_topology()