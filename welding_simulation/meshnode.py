import meshio
import json
from collections import defaultdict
from google.colab import files

def export_node_centric_mesh_for_kafka(mesh_file_path, output_jsonl_path):
    mesh = meshio.read(mesh_file_path)
    
    node_to_elements = defaultdict(list)
    element_id = 0
    
    for cell_block in mesh.cells:
        for element_nodes in cell_block.data:
            for node_id in element_nodes:
                node_to_elements[int(node_id)].append(element_id)
            element_id += 1
            
    with open(output_jsonl_path, 'w') as f:
        for node_id, coords in enumerate(mesh.points):
            kafka_message = {
                "node_id": node_id,
                "coordinates": {
                    "x": round(float(coords[0]), 5), 
                    "y": round(float(coords[1]), 5), 
                    "z": round(float(coords[2]), 5)
                },
                "connected_elements": node_to_elements[node_id]
            }
            f.write(json.dumps(kafka_message) + '\n')
            
    print(f"فایل با موفقیت تولید شد: {output_jsonl_path}")

# ۱. اجرای تابع (اسم فایل مش خود را اینجا قرار دهید)
export_node_centric_mesh_for_kafka('weld_mesh_final.msh', 'kafka_nodes_stream.jsonl')

# ۲. دستور دانلود مستقیم فایل از کولب به سیستم شما
files.download('kafka_nodes_stream.jsonl')