import os
import time
import json
import numpy as np
import pandas as pd
import streamlit as st
import plotly.graph_objects as go
from pymongo import MongoClient
import docker

# ---------------------------------------------------------
# Page Configuration
# ---------------------------------------------------------
st.set_page_config(
    page_title="3D Welding Digital Twin Monitor",
    page_icon="🔥",
    layout="wide",
)

# ---------------------------------------------------------
# CSS OVERRIDE: BALANCED TOP SPACING (~4mm offset)
# ---------------------------------------------------------
st.markdown("""
    <style>
        [data-testid="stSidebarHeader"] {
            padding: 0px !important;
            min-height: 0px !important;
            height: 0px !important;
            display: none !important;
        }
        
        [data-testid="stSidebarUserContent"] {
            padding-top: 1.2rem !important;
        }
        
        section[data-testid="stSidebar"] > div:first-child {
            padding-top: 1.2rem !important;
        }
        
        [data-testid="stSidebar"] h1 {
            margin-top: 0rem !important;
            padding-top: 0rem !important;
        }
    </style>
""", unsafe_allow_html=True)

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017/")
DB_NAME = "welding_digital_twin"
GEO_COLLECTION = "mesh_geometry"
TELEM_COLLECTION = "welding_telemetry"
THERMAL_COLLECTION = "welding_thermal"

METALLURGICAL_THRESHOLDS = {
    "melting_point": 1450.0,
    "yield_strength": 355.0,
    "critical_deflection": 0.003,
}

@st.cache_resource
def get_mongo_client():
    return MongoClient(MONGO_URI)

client = get_mongo_client()
db = client[DB_NAME]
geo_coll = db[GEO_COLLECTION]
telem_coll = db[TELEM_COLLECTION]
thermal_coll = db[THERMAL_COLLECTION]

@st.cache_data(ttl=60)
def load_base_geometry():
    nodes = list(geo_coll.find({}, {"_id": 0, "node_id": 1, "coordinates": 1, "connected_elements": 1}).sort("node_id", 1))
    if not nodes:
        return None
    df_geo = pd.DataFrame([n["coordinates"] for n in nodes])
    df_geo["node_id"] = [n["node_id"] for n in nodes]
    if "connected_elements" in nodes[0]:
        df_geo["connected_elements"] = [n.get("connected_elements", []) for n in nodes]
    return df_geo

# ---------------------------------------------------------
# Load Connectivity from JSONL File
# ---------------------------------------------------------
@st.cache_data
def load_mesh_connectivity(filepath="volumetric_elements.jsonl", max_nodes=50000):
    node_to_elems = {i: [] for i in range(max_nodes)}
    try:
        with open(filepath, 'r') as f:
            for line in f:
                record = json.loads(line.strip())
                elem_idx = record["element_id"] - 1 
                for node in record["nodes"]:
                    if node < max_nodes:
                        node_to_elems[node].append(elem_idx)
        return node_to_elems
    except FileNotFoundError:
        return None

@st.cache_data
def load_elements_data(filepath="volumetric_elements.jsonl"):
    elems = []
    try:
        with open(filepath, 'r') as f:
            for line in f:
                record = json.loads(line.strip())
                elems.append({
                    "element_id": record["element_id"],
                    "nodes": record["nodes"]
                })
        return elems
    except FileNotFoundError:
        return []

# ---------------------------------------------------------
# Navigation Sidebar
# ---------------------------------------------------------
st.sidebar.title("🔥 3D Welding Digital Twin")
st.sidebar.caption("High-Fidelity Thermomechanical Ingestion Pipeline (Kafka / MongoDB)")

page = st.sidebar.radio(
    "Select Telemetry View:",
    [
        "1. Live 3D Twin & Thermal-Stress Stream",
        "2. Thermomechanical Field Deep Dive",
        "3. Plasticity, Deformation & Stress Report",
        "4. Simulation Time-Step Archive (dt = 0.02s)",
        "5. Cluster Health & Pipeline Infrastructure",
    ],
)

# ---------------------------------------------------------
# Live Monitoring & Notification System
# ---------------------------------------------------------
times_telem = telem_coll.distinct("time")
times_thermal = thermal_coll.distinct("time")
global_available_times = sorted(list(set(times_telem + times_thermal)))

if "previous_frame_count" not in st.session_state:
    st.session_state.previous_frame_count = len(global_available_times)

current_frame_count = len(global_available_times)

if current_frame_count > st.session_state.previous_frame_count:
    if st.session_state.previous_frame_count > 0:
        new_frames = current_frame_count - st.session_state.previous_frame_count
        st.toast(f"🚀 Live Update: {new_frames} new time frames added to the stream!", icon="🔔")
    st.session_state.previous_frame_count = current_frame_count

st.sidebar.markdown("---")
if st.sidebar.button("🔄 Scan for New Data (Live Sync)"):
    st.rerun()

# ---------------------------------------------------------
# PAGE 1: INTERACTIVE 3D TWIN & TIMELINE SCRUBBER
# ---------------------------------------------------------
if page == "1. Live 3D Twin & Thermal-Stress Stream":
    st.markdown("### 🧊 Interactive 3D Digital Twin & Timeline Scrubber")
    st.markdown('<p style="font-size: 1.1rem; color: #555; margin-bottom: 10px;">Scrub through welding simulation history to inspect thermal fields and transient mechanical warpage.</p>', unsafe_allow_html=True)

    df_mesh = load_base_geometry()
    available_times = sorted(telem_coll.distinct("time"))

    if not available_times or df_mesh is None:
        st.info("Waiting for geometry and simulation frames in database...")
    else:
        total_frames = len(available_times)
        n_nodes = len(df_mesh)
        
        connectivity_map = load_mesh_connectivity(max_nodes=n_nodes)

        st.sidebar.markdown("---")
        st.sidebar.subheader("🎛️ Simulation & View Controls")

        frame_idx = st.sidebar.slider(
            "⏱️ Simulation Frame Progress:",
            min_value=0,
            max_value=total_frames - 1,
            value=total_frames - 1
        )
        selected_time = available_times[frame_idx]

        scale_factor = st.sidebar.slider(
            "🔍 Warpage Magnification Scale:",
            min_value=0.0,
            max_value=20.0,
            value=1.0,
            step=0.5
        )

        render_mode = st.sidebar.radio(
            "Select Mesh Display Style:",
            ["Continuous Surface (Solid Plate)", "Point Cloud (Nodal)", "Hybrid (Surface + Nodes)"]
        )

        color_mode = st.sidebar.radio(
            "Select 3D Color Contour:",
            ["Temperature Field (°C)", "Von Mises Stress (MPa)", "Material State (Chewing Gum)"]
        )

        current_frame = telem_coll.find_one({"time": selected_time})

        if current_frame:
            displacements_raw = np.array(current_frame.get("displacement_3d", []))
            temps_raw = np.array(current_frame.get("temperatures", []))
            stresses_raw = np.array(current_frame.get("von_mises_stress", [])) / 1e6
            states_raw = np.array(current_frame.get("material_state", []))

            def map_to_nodes(raw_array, is_state=False):
                if len(raw_array) == 0:
                    return np.zeros(n_nodes)
                if abs(len(raw_array) - n_nodes) < 10:
                    arr = np.zeros(n_nodes)
                    vlen = min(n_nodes, len(raw_array))
                    arr[:vlen] = raw_array[:vlen]
                    return arr
                if connectivity_map is not None:
                    nodal_arr = np.zeros(n_nodes)
                    for node_id in range(n_nodes):
                        connected_elems = connectivity_map.get(node_id, [])
                        valid_elems = [e for e in connected_elems if e < len(raw_array)]
                        if valid_elems:
                            if is_state:
                                nodal_arr[node_id] = np.max(raw_array[valid_elems])
                            else:
                                nodal_arr[node_id] = np.mean(raw_array[valid_elems])
                    return nodal_arr
                return np.zeros(n_nodes)

            temps = map_to_nodes(temps_raw)
            stresses = map_to_nodes(stresses_raw)
            states = map_to_nodes(states_raw, is_state=True)

            displacements = np.zeros((n_nodes, 3))
            if len(displacements_raw) > 0:
                vlen = min(n_nodes, len(displacements_raw))
                displacements[:vlen] = displacements_raw[:vlen]

            max_temp = float(np.max(temps)) if len(temps) > 0 else 0.0
            max_stress = float(np.max(stresses)) if len(stresses) > 0 else 0.0
            max_deflection = float(np.max(np.abs(displacements[:, 2]))) if len(displacements) > 0 else 0.0

            max_temp_node = int(np.argmax(temps)) if len(temps) > 0 else 0
            max_stress_node = int(np.argmax(stresses)) if len(stresses) > 0 else 0
            max_deflection_node = int(np.argmax(np.abs(displacements[:, 2]))) if len(displacements) > 0 else 0

            x_t, y_t, z_t = (df_mesh.loc[max_temp_node, ['x', 'y', 'z']].values) if n_nodes > max_temp_node else (0,0,0)
            x_s, y_s, z_s = (df_mesh.loc[max_stress_node, ['x', 'y', 'z']].values) if n_nodes > max_stress_node else (0,0,0)
            x_d, y_d, z_d = (df_mesh.loc[max_deflection_node, ['x', 'y', 'z']].values) if n_nodes > max_deflection_node else (0,0,0)

            is_molten = max_temp >= METALLURGICAL_THRESHOLDS["melting_point"]
            yield_exceeded = max_stress >= METALLURGICAL_THRESHOLDS["yield_strength"]

            if is_molten:
                st.warning(f"⚡ ACTIVE WELD POOL [t={selected_time:.2f}s | Frame {frame_idx + 1}/{total_frames}]: Peak Temp {max_temp:.1f} °C")
            else:
                st.success(f"❄️ COOLING / SOLID PHASE [t={selected_time:.2f}s | Frame {frame_idx + 1}/{total_frames}]: Monitoring Residual Stress")

            c1, c2, c3, c4, c5 = st.columns(5)
            
            with c1:
                st.markdown(f"""
                    <div style="line-height: 1.3;">
                        <span style="font-size: 0.8rem; color: #666;">Selected Time</span><br>
                        <span style="font-size: 1.5rem; font-weight: bold; color: #262730;">{selected_time:.2f} s</span><br>
                        <span style="font-size: 0.85rem; font-weight: bold; color: transparent;">-</span>
                    </div>
                """, unsafe_allow_html=True)

            with c2:
                badge_text = "Molten" if is_molten else "Solid"
                badge_bg = "#e6f4ea" if is_molten else "#e1f5fe"
                badge_fg = "#137333" if is_molten else "#0288d1"
                st.markdown(f"""
                    <div style="line-height: 1.3;">
                        <span style="font-size: 0.8rem; color: #666;">Peak Temperature</span><br>
                        <span style="font-size: 1.5rem; font-weight: bold; color: #262730;">{max_temp:.1f} °C</span> 
                        <span style="font-size: 0.7rem; background-color: {badge_bg}; color: {badge_fg}; padding: 1px 5px; border-radius: 4px;">{badge_text}</span><br>
                        <span style="font-size: 0.85rem; font-weight: bold; color: #333;">📍 N:{max_temp_node} | X:{x_t:.3f}, Y:{y_t:.3f}, Z:{z_t:.3f}</span>
                    </div>
                """, unsafe_allow_html=True)

            with c3:
                stress_text = "Plastic Yield" if yield_exceeded else "Elastic"
                stress_bg = "#fce8e6" if yield_exceeded else "#e6f4ea"
                stress_fg = "#c5221f" if yield_exceeded else "#137333"
                st.markdown(f"""
                    <div style="line-height: 1.3;">
                        <span style="font-size: 0.8rem; color: #666;">Peak Von Mises</span><br>
                        <span style="font-size: 1.5rem; font-weight: bold; color: #262730;">{max_stress:.1f} MPa</span> 
                        <span style="font-size: 0.7rem; background-color: {stress_bg}; color: {stress_fg}; padding: 1px 5px; border-radius: 4px;">{stress_text}</span><br>
                        <span style="font-size: 0.85rem; font-weight: bold; color: #333;">📍 N:{max_stress_node} | X:{x_s:.3f}, Y:{y_s:.3f}, Z:{z_s:.3f}</span>
                    </div>
                """, unsafe_allow_html=True)

            with c4:
                st.markdown(f"""
                    <div style="line-height: 1.3;">
                        <span style="font-size: 0.8rem; color: #666;">Max Z-Deflection</span><br>
                        <span style="font-size: 1.5rem; font-weight: bold; color: #262730;">{(max_deflection * 1000):.2f} mm</span><br>
                        <span style="font-size: 0.85rem; font-weight: bold; color: #333;">📍 N:{max_deflection_node} | X:{x_d:.3f}, Y:{y_d:.3f}, Z:{z_d:.3f}</span>
                    </div>
                """, unsafe_allow_html=True)

            with c5:
                st.markdown(f"""
                    <div style="line-height: 1.3;">
                        <span style="font-size: 0.8rem; color: #666;">Scale Applied</span><br>
                        <span style="font-size: 1.5rem; font-weight: bold; color: #262730;">{scale_factor:.1f}x</span><br>
                        <span style="font-size: 0.85rem; font-weight: bold; color: transparent;">-</span>
                    </div>
                """, unsafe_allow_html=True)
            
            st.markdown("<hr style='margin: 8px 0;'>", unsafe_allow_html=True)

            curr_x = df_mesh["x"].values + (displacements[:, 0] * scale_factor)
            curr_y = df_mesh["y"].values + (displacements[:, 1] * scale_factor)
            curr_z = df_mesh["z"].values + (displacements[:, 2] * scale_factor)

            if color_mode == "Temperature Field (°C)":
                color_vals = temps
                colorscale = "Inferno"
            elif color_mode == "Von Mises Stress (MPa)":
                color_vals = stresses
                colorscale = "Jet"
            else:
                color_vals = states
                colorscale = "Viridis"

            df_temp = pd.DataFrame({
                "orig_idx": np.arange(n_nodes),
                "rx": np.round(df_mesh["x"].values, 5),
                "ry": np.round(df_mesh["y"].values, 5),
                "z": df_mesh["z"].values
            })
            
            top_indices = df_temp.sort_values("z").groupby(["rx", "ry"])["orig_idx"].last().values
            bot_indices = df_temp.sort_values("z").groupby(["rx", "ry"])["orig_idx"].first().values
            
            top_indices = np.sort(top_indices)
            bot_indices = np.sort(bot_indices)

            fig_traces = []

            if render_mode in ["Continuous Surface (Solid Plate)", "Hybrid (Surface + Nodes)"]:
                surface_top = go.Mesh3d(
                    x=curr_x[top_indices],
                    y=curr_y[top_indices],
                    z=curr_z[top_indices],
                    intensity=color_vals[top_indices],
                    colorscale=colorscale,
                    colorbar=dict(title=color_mode),
                    delaunayaxis='z',
                    opacity=1.0 if render_mode == "Continuous Surface (Solid Plate)" else 0.85,
                    flatshading=False,
                    lighting=dict(ambient=0.8, diffuse=0.8, roughness=0.2, specular=0.3),
                    name="Top Surface"
                )
                fig_traces.append(surface_top)

                surface_bot = go.Mesh3d(
                    x=curr_x[bot_indices],
                    y=curr_y[bot_indices],
                    z=curr_z[bot_indices],
                    intensity=color_vals[bot_indices],
                    colorscale=colorscale,
                    showscale=False,
                    delaunayaxis='z',
                    opacity=0.95,
                    flatshading=False,
                    lighting=dict(ambient=0.7, diffuse=0.7, roughness=0.3, specular=0.2),
                    name="Bottom Surface"
                )
                fig_traces.append(surface_bot)

            if render_mode in ["Point Cloud (Nodal)", "Hybrid (Surface + Nodes)"]:
                point_cloud = go.Scatter3d(
                    x=curr_x,
                    y=curr_y,
                    z=curr_z,
                    mode="markers",
                    marker=dict(
                        size=2.5 if render_mode == "Point Cloud (Nodal)" else 1.5,
                        color=color_vals,
                        colorscale=colorscale,
                        colorbar=dict(title=color_mode) if render_mode == "Point Cloud (Nodal)" else None,
                        opacity=0.9
                    ),
                    text=[f"Node: {i}<br>Z: {z:.4f}m" for i, z in enumerate(curr_z)],
                    name="FEA Nodal Mesh"
                )
                fig_traces.append(point_cloud)

            fig_3d = go.Figure(data=fig_traces)
            fig_3d.update_layout(
                scene=dict(
                    xaxis_title="X (m) [Length]",
                    yaxis_title="Y (m) [Width]",
                    zaxis_title="Z (m) [Thickness + Warpage]",
                    aspectmode="data"
                ),
                margin=dict(l=0, r=0, b=0, t=10),
                height=720
            )
            st.plotly_chart(fig_3d, use_container_width=True)

# ---------------------------------------------------------
# PAGE 2: FIELD DEEP DIVE & MECHANICAL INSPECTION
# ---------------------------------------------------------
elif page == "2. Thermomechanical Field Deep Dive":
    st.title("🎯 Thermomechanical Field Deep Dive")
    st.caption("Component-level inspection of nodal stress, strain, and displacement fields")

    df_mesh = load_base_geometry()
    available_times = sorted(telem_coll.distinct("time"))

    if not available_times or df_mesh is None:
        st.info("Waiting for geometry and simulation frames in database...")
    else:
        n_nodes = len(df_mesh)
        connectivity_map = load_mesh_connectivity(max_nodes=n_nodes)

        min_y = float(df_mesh['y'].min()) if 'y' in df_mesh.columns else 0.0
        max_y = float(df_mesh['y'].max()) if 'y' in df_mesh.columns else 1.0
        mean_y = float(df_mesh['y'].mean()) if 'y' in df_mesh.columns else 0.0

        st.subheader("📍 Spatial Query: Select Transverse Distance")
        col_inp1, col_inp2 = st.columns(2)
        with col_inp1:
            target_y = st.number_input(
                "Enter Transverse Distance / Y-coordinate (m) from Weld Center:",
                min_value=min_y,
                max_value=max_y,
                value=mean_y,
                format="%.4f",
                step=0.001
            )
        with col_inp2:
            st.info(f"Mesh Y-Range: [{min_y:.4f}m, {max_y:.4f}m]")

        y_coords = df_mesh['y'].values
        closest_node_idx = int(np.argmin(np.abs(y_coords - target_y)))
        
        node_row = df_mesh.iloc[closest_node_idx]
        node_x = float(node_row.get('x', 0.0))
        node_y = float(node_row.get('y', 0.0))
        node_z = float(node_row.get('z', 0.0))

        st.success(f"🎯 **Nearest Node Found:** Node ID `{closest_node_idx}` | Coordinates -> X: {node_x:.4f}m, Y: {node_y:.4f}m, Z: {node_z:.4f}m")

        connected_elems = connectivity_map.get(closest_node_idx, []) if connectivity_map else []
        if connected_elems:
            selected_elem = connected_elems[0]
            st.info(f"🔗 Connected Elements: {len(connected_elems)} elements found. Automatically selected Element ID `{selected_elem + 1}` (0-based: `{selected_elem}`) for stress history.")
        else:
            selected_elem = None
            st.warning("⚠️ No connected elements found in connectivity map for this node.")

        st.markdown("---")
        st.subheader("📈 Transient History Curves (Temperature & Stress)")

        def map_to_nodes_p2(raw_array, is_state=False):
            if len(raw_array) == 0:
                return np.zeros(n_nodes)
            if abs(len(raw_array) - n_nodes) < 10:
                arr = np.zeros(n_nodes)
                vlen = min(n_nodes, len(raw_array))
                arr[:vlen] = raw_array[:vlen]
                return arr
            if connectivity_map is not None:
                nodal_arr = np.zeros(n_nodes)
                for node_id in range(n_nodes):
                    celems = connectivity_map.get(node_id, [])
                    valid_elems = [e for e in celems if e < len(raw_array)]
                    if valid_elems:
                        if is_state:
                            nodal_arr[node_id] = np.max(raw_array[valid_elems])
                        else:
                            nodal_arr[node_id] = np.mean(raw_array[valid_elems])
                return nodal_arr
            return np.zeros(n_nodes)

        history_data = []
        for t in available_times:
            frame = telem_coll.find_one({"time": t})
            if frame:
                temps_raw = np.array(frame.get("temperatures", []))
                stresses_raw = np.array(frame.get("von_mises_stress", [])) / 1e6

                nodal_temps = map_to_nodes_p2(temps_raw)
                temp_val = float(nodal_temps[closest_node_idx]) if closest_node_idx < len(nodal_temps) else 0.0

                stress_val = 0.0
                if selected_elem is not None and len(stresses_raw) > 0:
                    if selected_elem < len(stresses_raw):
                        stress_val = float(stresses_raw[selected_elem])

                history_data.append({
                    "Time (s)": t,
                    "Temperature (°C)": temp_val,
                    "Von Mises Stress (MPa)": stress_val
                })

        if history_data:
            df_history = pd.DataFrame(history_data)

            col_chart1, col_chart2 = st.columns(2)
            with col_chart1:
                st.markdown(f"#### 🌡️ Temperature History (Node {closest_node_idx})")
                st.line_chart(df_history.set_index("Time (s)")["Temperature (°C)"])

            with col_chart2:
                st.markdown(f"#### ⚡ Von Mises Stress History (Element {selected_elem + 1 if selected_elem is not None else 'N/A'})")
                st.line_chart(df_history.set_index("Time (s)")["Von Mises Stress (MPa)"])

            with st.expander("📋 View Raw History Data Table"):
                st.dataframe(df_history, use_container_width=True)
        else:
            st.warning("No telemetry time-history records found.")

# ---------------------------------------------------------
# PAGE 3: PLASTICITY, DEFORMATION & STRESS REPORT
# ---------------------------------------------------------
elif page == "3. Plasticity, Deformation & Stress Report":
    st.title("📊 Plasticity, Deformation & Stress Report")
    st.caption("Top 10 critical nodes/elements report at the end of welding time based on selected criterion")

    df_mesh = load_base_geometry()
    latest_frame = telem_coll.find_one(sort=[("time", -1)])

    if not latest_frame or df_mesh is None:
        st.info("Waiting for simulation frames in database...")
    else:
        sim_time = latest_frame.get("time", 0.0)
        st.subheader(f"⏱️ Evaluation Time: End of Simulation (t = {sim_time:.3f} s)")

        criterion = st.selectbox(
            "Select Evaluation Criterion:",
            [
                "Von Mises Stress (MPa)",
                "Absolute Deformation Magnitude (m)",
                "Equivalent Plastic Strain (PEEQ)"
            ]
        )

        n_nodes = len(df_mesh)
        elements_data = load_elements_data()

        if criterion == "Von Mises Stress (MPa)":
            raw_stresses = np.array(latest_frame.get("von_mises_stress", []))
            vals = raw_stresses / 1e6 if len(raw_stresses) > 0 else np.array([])
            
            if len(vals) > 0:
                is_elemental = (len(vals) != n_nodes)
                top_indices = np.argsort(vals)[-10:][::-1]

                report_rows = []
                for rank, idx in enumerate(top_indices, 1):
                    val = float(vals[idx])
                    if is_elemental and elements_data and idx < len(elements_data):
                        elem_nodes = elements_data[idx]["nodes"]
                        valid_nodes = [n for n in elem_nodes if n < n_nodes]
                        if valid_nodes:
                            sub_df = df_mesh.iloc[valid_nodes]
                            cx = float(sub_df['x'].mean())
                            cy = float(sub_df['y'].mean())
                            cz = float(sub_df['z'].mean())
                        else:
                            cx, cy, cz = 0.0, 0.0, 0.0
                        report_rows.append({
                            "Rank": rank,
                            "Type": "Element",
                            "ID": elements_data[idx]["element_id"],
                            "Value (MPa)": f"{val:.2f}",
                            "X (m)": f"{cx:.4f}",
                            "Y (m)": f"{cy:.4f}",
                            "Z (m)": f"{cz:.4f}"
                        })
                    else:
                        node_id = int(idx)
                        row = df_mesh.iloc[node_id] if node_id < n_nodes else df_mesh.iloc[0]
                        report_rows.append({
                            "Rank": rank,
                            "Type": "Node",
                            "ID": node_id,
                            "Value (MPa)": f"{val:.2f}",
                            "X (m)": f"{float(row.get('x', 0)):.4f}",
                            "Y (m)": f"{float(row.get('y', 0)):.4f}",
                            "Z (m)": f"{float(row.get('z', 0)):.4f}"
                        })
                df_report = pd.DataFrame(report_rows)
                st.dataframe(df_report, use_container_width=True)
            else:
                st.warning("No von Mises stress data available.")

        elif criterion == "Absolute Deformation Magnitude (m)":
            displacements_raw = np.array(latest_frame.get("displacement_3d", []))
            if len(displacements_raw) > 0:
                vals = np.linalg.norm(displacements_raw, axis=1)
                top_indices = np.argsort(vals)[-10:][::-1]

                report_rows = []
                for rank, node_id in enumerate(top_indices, 1):
                    val = float(vals[node_id])
                    row = df_mesh.iloc[node_id] if node_id < n_nodes else df_mesh.iloc[0]
                    report_rows.append({
                        "Rank": rank,
                        "Type": "Node",
                        "ID": int(node_id),
                        "Displacement (m)": f"{val:.6f}",
                        "Displacement (mm)": f"{val * 1000:.2f}",
                        "X (m)": f"{float(row.get('x', 0)):.4f}",
                        "Y (m)": f"{float(row.get('y', 0)):.4f}",
                        "Z (m)": f"{float(row.get('z', 0)):.4f}"
                    })
                df_report = pd.DataFrame(report_rows)
                st.dataframe(df_report, use_container_width=True)
            else:
                st.warning("No displacement data available.")

        elif criterion == "Equivalent Plastic Strain (PEEQ)":
            peeq_raw = np.array(latest_frame.get("equivalent_plastic_strain", []))
            vals = peeq_raw if len(peeq_raw) > 0 else np.array([])

            if len(vals) > 0:
                is_elemental = (len(vals) != n_nodes)
                top_indices = np.argsort(vals)[-10:][::-1]

                report_rows = []
                for rank, idx in enumerate(top_indices, 1):
                    val = float(vals[idx])
                    if is_elemental and elements_data and idx < len(elements_data):
                        elem_nodes = elements_data[idx]["nodes"]
                        valid_nodes = [n for n in elem_nodes if n < n_nodes]
                        if valid_nodes:
                            sub_df = df_mesh.iloc[valid_nodes]
                            cx = float(sub_df['x'].mean())
                            cy = float(sub_df['y'].mean())
                            cz = float(sub_df['z'].mean())
                        else:
                            cx, cy, cz = 0.0, 0.0, 0.0
                        report_rows.append({
                            "Rank": rank,
                            "Type": "Element",
                            "ID": elements_data[idx]["element_id"],
                            "PEEQ Value": f"{val:.4f}",
                            "X (m)": f"{cx:.4f}",
                            "Y (m)": f"{cy:.4f}",
                            "Z (m)": f"{cz:.4f}"
                        })
                    else:
                        node_id = int(idx)
                        row = df_mesh.iloc[node_id] if node_id < n_nodes else df_mesh.iloc[0]
                        report_rows.append({
                            "Rank": rank,
                            "Type": "Node",
                            "ID": node_id,
                            "PEEQ Value": f"{val:.4f}",
                            "X (m)": f"{float(row.get('x', 0)):.4f}",
                            "Y (m)": f"{float(row.get('y', 0)):.4f}",
                            "Z (m)": f"{float(row.get('z', 0)):.4f}"
                        })
                df_report = pd.DataFrame(report_rows)
                st.dataframe(df_report, use_container_width=True)
            else:
                st.warning("No plastic strain data available.")

# ---------------------------------------------------------
# PAGE 4: SIMULATION TIME-STEP ARCHIVE (SORTED BY TEMPERATURE)
# ---------------------------------------------------------
elif page == "4. Simulation Time-Step Archive (dt = 0.02s)":
    st.title("⏱️ Simulation Time-Step Archive")
    st.caption("Discrete query interface for high-resolution 50 Hz thermomechanical frames (Sorted by Temperature)")

    df_mesh = load_base_geometry()
    distinct_times = telem_coll.distinct("time")
    
    if distinct_times and df_mesh is not None:
        distinct_times.sort()
        selected_time = st.select_slider(
            "Select Simulation Time (s):",
            options=distinct_times,
            value=distinct_times[-1]
        )

        frame = telem_coll.find_one({"time": selected_time})
        if frame:
            n_nodes = len(df_mesh)
            connectivity_map = load_mesh_connectivity(max_nodes=n_nodes)

            temps_raw = np.array(frame.get("temperatures", []))
            stresses_raw = np.array(frame.get("von_mises_stress", [])) / 1e6
            displacements_raw = np.array(frame.get("displacement_3d", []))

            # Mapping temperatures to nodes
            def map_to_nodes_p4(raw_array, is_elem=False):
                if len(raw_array) == 0:
                    return np.zeros(n_nodes)
                if abs(len(raw_array) - n_nodes) < 10:
                    arr = np.zeros(n_nodes)
                    vlen = min(n_nodes, len(raw_array))
                    arr[:vlen] = raw_array[:vlen]
                    return arr
                if connectivity_map is not None:
                    nodal_arr = np.zeros(n_nodes)
                    for node_id in range(n_nodes):
                        celems = connectivity_map.get(node_id, [])
                        valid_elems = [e for e in celems if e < len(raw_array)]
                        if valid_elems:
                            nodal_arr[node_id] = np.mean(raw_array[valid_elems])
                    return nodal_arr
                return np.zeros(n_nodes)

            temps = map_to_nodes_p4(temps_raw)
            stresses = map_to_nodes_p4(stresses_raw)

            displacements = np.zeros((n_nodes, 3))
            if len(displacements_raw) > 0:
                vlen = min(n_nodes, len(displacements_raw))
                displacements[:vlen] = displacements_raw[:vlen]

            col1, col2, col3 = st.columns(3)
            col1.metric("Max Temperature", f"{np.max(temps):.2f} °C" if len(temps) > 0 else "N/A")
            col2.metric("Max Von Mises Stress", f"{np.max(stresses):.2f} MPa" if len(stresses) > 0 else "N/A")
            col3.metric("Max Deflection", f"{(np.max(np.abs(displacements[:, 2]))*1000):.2f} mm" if len(displacements) > 0 else "N/A")

            st.markdown("---")
            st.subheader(f"🔥 Top Hottest Nodes Summary at t = {selected_time:.3f} s (Sorted by Temperature)")

            if len(temps) > 0:
                sorted_temp_indices = np.argsort(temps)[::-1]
                top_indices = sorted_temp_indices[:10]  # Top 10 hottest nodes

                report_rows = []
                for rank, node_id in enumerate(top_indices, 1):
                    t_val = float(temps[node_id]) if node_id < len(temps) else 0.0
                    s_val = float(stresses[node_id]) if node_id < len(stresses) else 0.0
                    d_val = float(displacements[node_id, 2]) if node_id < len(displacements) else 0.0
                    row = df_mesh.iloc[node_id] if node_id < n_nodes else df_mesh.iloc[0]

                    report_rows.append({
                        "Rank": rank,
                        "Node ID": int(node_id),
                        "Temperature (°C)": f"{t_val:.2f}",
                        "Stress (MPa)": f"{s_val:.2f}",
                        "Displacement Z (m)": f"{d_val:.6f}",
                        "X (m)": f"{float(row.get('x', 0)):.4f}",
                        "Y (m)": f"{float(row.get('y', 0)):.4f}",
                        "Z (m)": f"{float(row.get('z', 0)):.4f}"
                    })

                df_summary = pd.DataFrame(report_rows)
                st.dataframe(df_summary, use_container_width=True)
            else:
                st.warning("No temperature data available for this frame.")
    else:
        st.warning("No time-step frames or geometry found in database collection.")

# ---------------------------------------------------------
# PAGE 5: CLUSTER HEALTH & INFRASTRUCTURE MONITOR
# ---------------------------------------------------------
elif page == "5. Cluster Health & Pipeline Infrastructure":
    st.title("🖥️ Cluster Health & Infrastructure Monitor")
    st.caption("Real-time operational health inspection of Docker container services")

    try:
        docker_client = docker.from_env()
        containers = docker_client.containers.list(all=True)

        container_data = []
        running_count = 0
        total_count = len(containers)

        for c in containers:
            status = c.status
            if status == "running":
                running_count += 1

            ports = ", ".join([f"{k}->{v[0]['HostPort']}" for k, v in c.ports.items() if v]) if c.ports else "None"

            container_data.append({
                "Container Name": c.name,
                "Status": status.upper(),
                "Image": c.image.tags[0] if c.image.tags else c.image.short_id,
                "Ports": ports,
                "Created At": c.attrs.get("Created", "")[:19].replace("T", " "),
            })

        df_containers = pd.DataFrame(container_data)

        st.markdown("---")
        c1, c2, c3 = st.columns(3)
        c1.metric("Total Containers", f"{total_count}")
        c2.metric("Running Services", f"{running_count}", delta=f"{running_count}/{total_count} Healthy")
        c3.metric("Service Health Status", "All Operational" if running_count == total_count else "Degraded",
                  delta_color="normal" if running_count == total_count else "inverse")

        st.markdown("---")
        st.subheader("📋 Pipeline Service Architecture Status")

        def highlight_status(row):
            styles = [""] * len(row)
            cols = list(row.index)
            if "Status" in cols:
                if row["Status"] == "RUNNING":
                    styles[cols.index("Status")] = "background-color: #28a745; color: white; font-weight: bold;"
                else:
                    styles[cols.index("Status")] = "background-color: #dc3545; color: white; font-weight: bold;"
            return styles

        styled_df = df_containers.style.apply(highlight_status, axis=1)
        st.dataframe(styled_df, use_container_width=True)

    except Exception as e:
        st.error(f"Unable to connect to Docker daemon: {e}")
        st.info("Ensure `/var/run/docker.sock:/var/run/docker.sock` is mounted in `docker-compose.yml`.")
