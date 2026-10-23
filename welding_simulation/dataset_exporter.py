import json
import numpy as np
from pathlib import Path

class FEniCSxJSONExporter:
    def __init__(self, out_dir):
        self.out_dir = Path(out_dir)
        self.out_dir.mkdir(parents=True, exist_ok=True)
        
        # Use JSONL (JSON Lines) for Kafka streaming and memory efficiency
        self.thermal_file = self.out_dir / "thermal_series.jsonl"
        self.mechanical_file = self.out_dir / "mechanical_results.jsonl"
        
        # Clear previous files if they exist to prevent appending to old data
        if self.thermal_file.exists():
            self.thermal_file.unlink()
        if self.mechanical_file.exists():
            self.mechanical_file.unlink()

    def export_topology(self, mesh):
        """Extract node coordinates and element connectivity for GNN"""
        nodes = np.round(mesh.geometry.x, 6).tolist()
        elements = mesh.geometry.dofmap.tolist()
        
        topology_data = {
            "num_nodes": len(nodes),
            "num_elements": len(elements),
            "element_type": "hexahedron", # یا نوع مش فعلی
            "nodes": nodes,
            "elements": elements
        }
        
        with open(self.out_dir / "mesh_topology.json", "w") as f:
            json.dump(topology_data, f)
        print(f"Topology exported: {len(nodes)} nodes, {len(elements)} elements.")

    def append_thermal_step(self, time, temperature_function, material_state_function=None):
        """Stream nodal/cell data along with material state directly to disk (JSONL format)"""
        t_array = np.round(temperature_function.x.array.real, 3).tolist()
        
        step_data = {
            "time": round(time, 3),
            "temperatures": t_array
        }
        
        if material_state_function is not None:
            step_data["material_state"] = np.round(material_state_function.x.array.real, 3).tolist()
        
        # Append as a single JSON line for Kafka streaming
        with open(self.thermal_file, "a") as f:
            f.write(json.dumps(step_data) + "\n")

    def dump_thermal_history(self, verbose=True):
        """Deprecated: Logic moved to append method. Kept for compatibility."""
        if verbose:
            print("Thermal series streamed to JSONL successfully.")

    def append_mechanical_step(
        self, 
        time, 
        displacement, 
        eqp_function=None, 
        temperature_function=None, 
        stress_function=None, 
        material_state_function=None,
        stress_tensor_function=None
    ):
        """Stream mechanical data including full stress tensor and material state directly to disk (JSONL format)"""
        u_array = np.round(displacement.x.array.real.reshape(-1, 3), 6).tolist()
        
        step_data = {
            "time": round(time, 3),
            "displacement_3d": u_array
        }
        
        if eqp_function is not None:
            step_data["equivalent_plastic_strain"] = np.round(eqp_function.x.array.real, 5).tolist()
            
        if temperature_function is not None:
            step_data["temperatures"] = np.round(temperature_function.x.array.real, 3).tolist()
            
        if stress_function is not None:
            step_data["von_mises_stress"] = np.round(stress_function.x.array.real, 2).tolist()

        if material_state_function is not None:
            step_data["material_state"] = np.round(material_state_function.x.array.real, 3).tolist()

        if stress_tensor_function is not None:
            # Reshape tensor components to (N, 3, 3) matrix format per cell/node
            tensor_arr = stress_tensor_function.x.array.real
            if tensor_arr.size == len(tensor_arr) * 9:
                tensor_arr = tensor_arr.reshape(-1, 3, 3)
            step_data["stress_tensor"] = np.round(tensor_arr, 2).tolist()

        # Append as a single JSON line for Kafka streaming
        with open(self.mechanical_file, "a") as f:
            f.write(json.dumps(step_data) + "\n")

    def dump_mechanical_history(self, verbose=True):
        """Deprecated: Logic moved to append method. Kept for compatibility."""
        if verbose:
            print("Mechanical series streamed to JSONL successfully.")