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
# CSS OVERRIDE: BALANCED TOP SPACING, UI LABELS & HTML TABLES
# ---------------------------------------------------------
st.markdown("""
    <style>
        [data-testid="stSidebarHeader"] {
            padding: 0px !important;
            min-height: 0px !important;
            height: 0px !important;
            display: none !important;
        }

        /* Slider thumb value (the red number) and min/max tick numbers */
        [data-testid="stSlider"] [data-testid="stThumbValue"] {
            font-size: 1.25rem !important;
            font-weight: bold !important;
        }
        [data-testid="stSlider"] [data-testid="stTickBar"] div,
        [data-testid="stSlider"] [data-testid="stTickBarMin"],
        [data-testid="stSlider"] [data-testid="stTickBarMax"] {
            font-size: 1.1rem !important;
            color: #333 !important;
            font-weight: 500 !important;
        }

        /* Metric card labels and values */
        [data-testid="stMetricLabel"] p {
            font-size: 1.15rem !important;
            font-weight: 600 !important;
            color: #262730 !important;
        }
        [data-testid="stMetricValue"] div {
            font-size: 1.8rem !important;
            font-weight: bold !important;
        }

        /* Increase font size and padding for expander headers */
        [data-testid="stExpander"] details summary p {
            font-size: 1.2rem !important;
            font-weight: 600 !important;
            color: #262730 !important;
        }
        [data-testid="stExpander"] details summary svg {
            width: 1.3rem !important;
            height: 1.3rem !important;
        }

        /* Selectbox value inside the box */
        div[data-baseweb="select"] div {
            font-size: 1.2rem !important;
        }

        /* Selectbox dropdown popup menu items */
        ul[role="listbox"] li,
        ul[role="listbox"] div {
            font-size: 1.2rem !important;
            line-height: 1.8 !important;
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
        
        /* Increase font size for Streamlit alert boxes (info, success, warning) */
        [data-testid="stAlert"] div[role="alert"] p,
        [data-testid="stAlert"] div[role="alert"] span,
        [data-testid="stAlert"] code {
            font-size: 1.15rem !important;
            line-height: 1.6 !important;
        }

        /* Increase font size for number inputs, selectboxes, and sliders */
        [data-testid="stNumberInput"] label p,
        [data-testid="stSelectbox"] label p,
        [data-testid="stSlider"] label p {
            font-size: 1.15rem !important;
            font-weight: 600 !important;
            color: #262730 !important;
        }

        [data-testid="stNumberInput"] input {
            font-size: 1.25rem !important;
            padding: 8px !important;
        }

        /* Increase font size for selectbox options */
        /* Selectbox value inside input box */
        div[data-baseweb="select"] * {
            font-size: 1.35rem !important;
            font-weight: 500 !important;
            color: #262730 !important;
        }

        /* Selectbox dropdown popup menu items */
        div[data-baseweb="popover"] li,
        div[data-baseweb="popover"] li * {
            font-size: 1.35rem !important;
            font-weight: 500 !important;
            line-height: 1.8 !important;
            color: #262730 !important;
        }
        
        /* Custom HTML Table Styling for Large, Readable Fonts */
        .custom-html-table {
            width: 100%;
            border-collapse: collapse;
            font-size: 1.25rem !important;
            font-family: Arial, sans-serif;
            margin-bottom: 1rem;
        }
        .custom-html-table th {
            background-color: #f0f2f6 !important;
            color: #111 !important;
            font-size: 1.3rem !important;
            font-weight: bold !important;
            padding: 14px 12px !important;
            text-align: left;
            border-bottom: 2px solid #b0b0b0 !important;
        }
        .custom-html-table td {
            font-size: 1.25rem !important;
            font-weight: 500 !important;
            padding: 12px 12px !important;
            border-bottom: 1px solid #dcdcdc !important;
            color: #262730 !important;
        }
        .custom-html-table tr:hover {
            background-color: #f1f3f6 !important;
        }

        /* Slider and Select-Slider Labels */
        [data-testid="stSlider"] label,
        [data-testid="stSlider"] label p {
            font-size: 1.25rem !important;
            font-weight: 600 !important;
            color: #262730 !important;
        }

        /* Value above the thumb (the red number) */
        div[data-baseweb="slider"] div[role="slider"] div,
        div[data-baseweb="slider"] div[aria-hidden="true"],
        [data-testid="stSlider"] div[data-testid="stThumbValue"] {
            font-size: 1.35rem !important;
            font-weight: bold !important;
            line-height: 1.5 !important;
        }

        /* Min, Max and Tick values below the track */
        div[data-baseweb="slider"] > div:last-child div,
        [data-testid="stSlider"] [data-testid="stTickBar"] div,
        [data-testid="stSlider"] [data-testid="stTickBarMin"],
        [data-testid="stSlider"] [data-testid="stTickBarMax"] {
            font-size: 1.2rem !important;
            color: #262730 !important;
            font-weight: 600 !important;
        }

        /* Streamlit Alerts (st.success, st.info, st.warning) Complete Override */
        [data-testid="stAlert"] {
            padding: 14px 18px !important;
        }
        [data-testid="stAlert"] p,
        [data-testid="stAlert"] span,
        [data-testid="stAlert"] div {
            font-size: 1.4rem !important;
            line-height: 1.7 !important;
            font-weight: 500 !important;
        }
        [data-testid="stAlert"] code {
            font-size: 1.35rem !important;
            font-weight: bold !important;
        }

        /* Increase font size for number inputs and labels */
        [data-testid="stNumberInput"] label,
        [data-testid="stNumberInput"] label p {
            font-size: 1.35rem !important;
            font-weight: 600 !important;
            color: #262730 !important;
        }
        [data-testid="stNumberInput"] input {
            font-size: 1.4rem !important;
            font-weight: 600 !important;
            padding: 10px !important;
        }
        [data-testid="stNumberInput"] button {
            transform: scale(1.15) !important;
        }

        /* Sidebar subtitle / caption */
        section[data-testid="stSidebar"] p {
            font-size: 1.3rem !important;
            line-height: 1.6 !important;
        }

        /* Radio button labels & option text */
        /* Radio button labels & all option text */
        [data-testid="stRadio"] label,
        [data-testid="stRadio"] label p,
        [data-testid="stRadio"] div[role="radiogroup"] label,
        [data-testid="stRadio"] div[role="radiogroup"] label * {
            font-size: 1.35rem !important;
            font-weight: 600 !important;
            color: #262730 !important;
            line-height: 1.5 !important;
        }

        /* Checkbox label */
        [data-testid="stCheckbox"] label,
        [data-testid="stCheckbox"] label p {
            font-size: 1.35rem !important;
            font-weight: 600 !important;
            color: #262730 !important;
        }

        /* Sidebar action button */
        section[data-testid="stSidebar"] button,
        section[data-testid="stSidebar"] button p {
            font-size: 1.3rem !important;
            font-weight: bold !important;
            padding: 4px 8px !important;
        }
    </style>
""", unsafe_allow_html=True)

def render_html_table(df):
    if df is None or df.empty:
        st.warning("No data available to display.")
        return
    html_str = df.to_html(classes="custom-html-table", index=True, border=0, escape=False)
    st.markdown(f'<div style="max-height: 500px; overflow-y: auto;">{html_str}</div>', unsafe_allow_html=True)

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017/")
DB_NAME = "welding_digital_twin"
GEO_COLLECTION = "mesh_geometry"
TELEM_COLLECTION = "welding_telemetry"
THERMAL_COLLECTION = "welding_thermal"

DATA_DIR = os.getenv("DATA_DIR", "/app/data")
if not os.path.exists(DATA_DIR):
    if os.path.exists("result_data_welding"):
        DATA_DIR = "result_data_welding"
    else:
        DATA_DIR = "."
MESH_FILE_PATH = os.path.join(DATA_DIR, "volumetric_elements.jsonl")

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
# Load Connectivity and Elements
# ---------------------------------------------------------
@st.cache_data
def load_mesh_connectivity(filepath=MESH_FILE_PATH, max_nodes=50000):
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
def load_elements_data(filepath=MESH_FILE_PATH):
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

@st.cache_data
def get_hex8_mesh_topology(filepath=MESH_FILE_PATH):
    raw_elements = load_elements_data(filepath)
    if not raw_elements:
        return None, None

    face_defs = [
        [0, 1, 3, 2],
        [4, 6, 7, 5],
        [0, 1, 5, 4],
        [2, 6, 7, 3],
        [0, 4, 6, 2],
        [1, 3, 7, 5]
    ]

    edge_defs = [
        (0, 1), (1, 3), (3, 2), (2, 0),
        (4, 5), (5, 7), (7, 6), (6, 4),
        (0, 4), (1, 5), (2, 6), (3, 7)
    ]

    face_count = {}
    edges_set = set()

    for elem in raw_elements:
        nodes = elem["nodes"]
        for f in face_defs:
            orig_quad = [nodes[i] for i in f]
            face_key = tuple(sorted(orig_quad))
            if face_key in face_count:
                face_count[face_key][0] += 1
            else:
                face_count[face_key] = [1, orig_quad]

        for u, v in edge_defs:
            p1, p2 = nodes[u], nodes[v]
            edges_set.add(tuple(sorted((p1, p2))))

    tri_i, tri_j, tri_k = [], [], []
    for count, quad in face_count.values():
        if count == 1:
            tri_i.extend([quad[0], quad[0]])
            tri_j.extend([quad[1], quad[2]])
            tri_k.extend([quad[2], quad[3]])

    return (np.array(tri_i), np.array(tri_j), np.array(tri_k)), list(edges_set)

# ---------------------------------------------------------
# Navigation Sidebar
# ---------------------------------------------------------
st.sidebar.title("🔥 3D Welding Digital Twin")
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
if "color_mode" not in st.session_state:
    st.session_state.color_mode = "Temperature Field (°C)"
current_frame_count = len(global_available_times)

if current_frame_count > st.session_state.previous_frame_count:
    if st.session_state.previous_frame_count > 0:
        new_frames = current_frame_count - st.session_state.previous_frame_count
        st.toast(f"🚀 Live Update: {new_frames} new time frames added to the stream!", icon="🔔")
    st.session_state.previous_frame_count = current_frame_count

st.sidebar.markdown("---")
if st.sidebar.button("🔄 Scan for New Data (Live Sync)"):
    st.toast("🔄 Data synchronized with MongoDB!", icon="✅")

# ---------------------------------------------------------
# PAGE 1: INTERACTIVE 3D TWIN & TIMELINE SCRUBBER
# ---------------------------------------------------------
if page == "1. Live 3D Twin & Thermal-Stress Stream":
    st.markdown("### 🧊 Interactive 3D Digital Twin & Timeline Scrubber")
    st.markdown('<p style="font-size: 1.25rem; color: #333; margin-bottom: 15px;">Scrub through welding simulation history to inspect thermal fields and transient mechanical warpage.</p>', unsafe_allow_html=True)

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
            [
                "Solid 3D Block (Volume)",
                "Point Cloud (Nodal)",
                "Dual Shell (Top/Bottom Surfaces)"
            ]
        )

        show_wireframe = st.sidebar.checkbox(
            "Show 3D Element Edges (Wireframe)",
            value=False,
            help="Render mesh element boundaries with subtle lines"
        )

        color_mode = st.sidebar.radio(
            "Select 3D Color Contour:",
            ["Temperature Field (°C)", "Von Mises Stress (MPa)", "Material State (Chewing Gum)"],
            key="color_mode"
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

            fig_traces = []

            if render_mode == "Solid 3D Block (Volume)":
                tri_indices, _ = get_hex8_mesh_topology()
                if tri_indices is not None:
                    ti, tj, tk = tri_indices
                    solid_block = go.Mesh3d(
                        x=curr_x,
                        y=curr_y,
                        z=curr_z,
                        i=ti,
                        j=tj,
                        k=tk,
                        intensity=color_vals,
                        colorscale=colorscale,
                        colorbar=dict(
                            title=dict(text=color_mode, font=dict(size=18, family="Arial", color="black")),
                            tickfont=dict(size=16, family="Arial", color="black"),
                            thickness=22,
                            len=0.55,
                            y=0.5,
                            yanchor="middle",
                        ),
                        opacity=1.0,
                        flatshading=True,
                        lighting=dict(ambient=0.85, diffuse=0.6, roughness=0.5, specular=0.1),
                        name="Solid 3D Block",
                    )
                    fig_traces.append(solid_block)

            elif render_mode == "Dual Shell (Top/Bottom Surfaces)":
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

                surface_top = go.Mesh3d(
                    x=curr_x[top_indices],
                    y=curr_y[top_indices],
                    z=curr_z[top_indices],
                    intensity=color_vals[top_indices],
                    colorscale=colorscale,
                    colorbar=dict(
                        title=dict(text=color_mode, font=dict(size=18, family="Arial", color="black")),
                        tickfont=dict(size=16, family="Arial", color="black"),
                        thickness=22,
                        len=0.55,
                        y=0.5,
                        yanchor="middle",
                    ),
                    delaunayaxis='z',
                    opacity=1.0,
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

            elif render_mode == "Point Cloud (Nodal)":
                point_cloud = go.Scatter3d(
                    x=curr_x,
                    y=curr_y,
                    z=curr_z,
                    mode="markers",
                    marker=dict(
                        size=2.5,
                        color=color_vals,
                        colorscale=colorscale,
                        colorbar=dict(
                            title=dict(text=color_mode, font=dict(size=18, family="Arial", color="black")),
                            tickfont=dict(size=16, family="Arial", color="black"),
                            thickness=22,
                            len=0.55,
                            y=0.5,
                            yanchor="middle",
                        ),
                        opacity=0.9
                    ),
                    text=[f"Node: {i}<br>Z: {z:.4f}m" for i, z in enumerate(curr_z)],
                    name="FEA Nodal Mesh"
                )
                fig_traces.append(point_cloud)

            if show_wireframe:
                _, edges_list = get_hex8_mesh_topology()
                if edges_list:
                    edge_x, edge_y, edge_z = [], [], []
                    for n1, n2 in edges_list:
                        if n1 < n_nodes and n2 < n_nodes:
                            edge_x.extend([curr_x[n1], curr_x[n2], None])
                            edge_y.extend([curr_y[n1], curr_y[n2], None])
                            edge_z.extend([curr_z[n1], curr_z[n2], None])

                    wireframe_trace = go.Scatter3d(
                        x=edge_x,
                        y=edge_y,
                        z=edge_z,
                        mode="lines",
                        line=dict(color="rgba(100, 100, 100, 0.25)", width=1.2),
                        hoverinfo="none",
                        name="Element Edges"
                    )
                    fig_traces.append(wireframe_trace)

            fig_3d = go.Figure(data=fig_traces)
            fig_3d.update_layout(
                font=dict(
                    family="Arial, sans-serif",
                    size=16,
                    color="black"
                ),
                scene=dict(
                    xaxis_title=dict(text="X (m) [Length]", font=dict(size=16, color="black")),
                    yaxis_title=dict(text="Y (m) [Width]", font=dict(size=16, color="black")),
                    zaxis=dict(
                        title=dict(text="Z (m) [Thickness: 3mm + Warpage]", font=dict(size=15, color="black")),
                        showticklabels=False,
                        showgrid=True,
                        zeroline=True
                    ),
                    aspectmode="manual",
                    aspectratio=dict(x=1.2, y=1.0, z=0.04),
                    camera=dict(
                        eye=dict(x=1.2, y=-1.3, z=0.9),
                        center=dict(x=0, y=0, z=-0.1)
                    )
                ),
                margin=dict(l=0, r=0, b=0, t=10),
                height=750
            )
            st.plotly_chart(
                fig_3d,
                use_container_width=True,
                config={
                    'toImageButtonOptions': {
                        'format': 'png',
                        'filename': f'3D_Welding_t_{selected_time:.2f}s',
                        'height': 900,
                        'width': 1400,
                        'scale': 3
                    }
                }
            )

# ---------------------------------------------------------
# PAGE 2: FIELD DEEP DIVE & MECHANICAL INSPECTION
# ---------------------------------------------------------
elif page == "2. Thermomechanical Field Deep Dive":
    st.title("🎯 Thermomechanical Field Deep Dive")
    st.markdown('<p style="font-size: 1.25rem; color: #333; margin-top: -10px; margin-bottom: 20px;">Component-level inspection of nodal stress, strain, and displacement fields</p>', unsafe_allow_html=True)

    df_mesh = load_base_geometry()
    available_times = sorted(telem_coll.distinct("time"))

    if not available_times or df_mesh is None:
        st.info("Waiting for geometry and simulation frames in database...")
    else:
        n_nodes = len(df_mesh)
        connectivity_map = load_mesh_connectivity(max_nodes=n_nodes)

        min_y = float(df_mesh['y'].min()) if 'y' in df_mesh.columns else 0.0
        max_y = float(df_mesh['y'].max()) if 'y' in df_mesh.columns else 1.0
        center_y = (min_y + max_y) / 2.0

        st.subheader("📍 Spatial Query: Select Distance from Weld Center")
        col_inp1, col_inp2 = st.columns(2)
        with col_inp1:
            offset_y = st.number_input(
                "Enter Distance from Weld Center (m):",
                min_value=0.0,
                max_value=(max_y - min_y) / 2.0,
                value=0.0,
                format="%.4f",
                step=0.001
            )
        with col_inp2:
            st.info(f"Weld Center Y: {center_y:.4f}m | Max Offset: {((max_y - min_y) / 2.0):.4f}m")

        target_y = center_y + offset_y

        x_coords = df_mesh['x'].values
        y_coords = df_mesh['y'].values
        z_coords = df_mesh['z'].values
        
        target_x = (x_coords.min() + x_coords.max()) / 2.0
        target_z = z_coords.max()
        
        mask = (np.abs(x_coords - target_x) < 0.005) & (np.abs(z_coords - target_z) < 0.001)
        if not np.any(mask):
            mask = np.ones_like(x_coords, dtype=bool)
            
        sub_indices = np.where(mask)[0]
        sub_y = y_coords[sub_indices]
        
        local_closest = int(np.argmin(np.abs(sub_y - target_y)))
        closest_node_idx = sub_indices[local_closest]
        
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

        col_chart1, col_chart2 = st.columns(2)
        with col_chart1:
            st.markdown(f"#### 🌡️ Temperature History (Node {closest_node_idx})")
            telem_docs = list(telem_coll.find({}, {"time": 1, "temperatures": 1}).sort("time", 1))
            thermal_rows = []
            for doc in telem_docs:
                t_val = doc.get("time", 0.0)
                temps_raw = np.array(doc.get("temperatures", []))
                temp_val = 0.0
                if len(temps_raw) > 0:
                    node_idx = closest_node_idx if (closest_node_idx is not None and 0 <= closest_node_idx < len(temps_raw)) else 0
                    temp_val = float(temps_raw[node_idx])
                thermal_rows.append({"Time (s)": t_val, "Temperature (°C)": temp_val})
            
            df_thermal_raw = pd.DataFrame(thermal_rows)
            if not df_thermal_raw.empty:
                fig_temp = go.Figure(data=go.Scatter(
                    x=df_thermal_raw["Time (s)"],
                    y=df_thermal_raw["Temperature (°C)"],
                    mode='lines',
                    line=dict(color="#1f77b4", width=3)
                ))
                fig_temp.update_layout(
                    font=dict(family="Arial, sans-serif", size=16, color="black"),
                    xaxis=dict(title=dict(text="Time (s)", font=dict(size=18, color="black")), tickfont=dict(size=14, color="black")),
                    yaxis=dict(title=dict(text="Temperature (°C)", font=dict(size=18, color="black")), tickfont=dict(size=14, color="black")),
                    margin=dict(l=40, r=20, t=20, b=40)
                )
                st.plotly_chart(
                    fig_temp,
                    use_container_width=True,
                    config={
                        'toImageButtonOptions': {
                            'format': 'png',
                            'filename': f'Temperature_History_Node_{closest_node_idx}',
                            'height': 600,
                            'width': 900,
                            'scale': 3
                        }
                    }
                )
            else:
                st.warning("No thermal history records found.")

        with col_chart2:
            st.markdown(f"#### ⚡ Von Mises Stress History (Element {selected_elem + 1 if selected_elem is not None else 'N/A'})")
            telem_docs = list(telem_coll.find({}, {"time": 1, "von_mises_stress": 1}).sort("time", 1))
            stress_rows = []
            for doc in telem_docs:
                t_val = doc.get("time", 0.0)
                stresses = np.array(doc.get("von_mises_stress", [])) / 1e6
                if selected_elem is not None and len(stresses) > 0 and selected_elem < len(stresses):
                    s_val = float(stresses[selected_elem])
                else:
                    s_val = float(np.max(stresses)) if len(stresses) > 0 else 0.0
                stress_rows.append({"Time (s)": t_val, "Von Mises Stress (MPa)": s_val})
            
            df_stress_raw = pd.DataFrame(stress_rows)
            if not df_stress_raw.empty:
                fig_stress = go.Figure(data=go.Scatter(
                    x=df_stress_raw["Time (s)"],
                    y=df_stress_raw["Von Mises Stress (MPa)"],
                    mode='lines',
                    line=dict(color="#d62728", width=3)
                ))
                fig_stress.update_layout(
                    font=dict(family="Arial, sans-serif", size=16, color="black"),
                    xaxis=dict(title=dict(text="Time (s)", font=dict(size=18, color="black")), tickfont=dict(size=14, color="black")),
                    yaxis=dict(title=dict(text="Von Mises Stress (MPa)", font=dict(size=18, color="black")), tickfont=dict(size=14, color="black")),
                    margin=dict(l=40, r=20, t=20, b=40)
                )
                st.plotly_chart(
                    fig_stress,
                    use_container_width=True,
                    config={
                        'toImageButtonOptions': {
                            'format': 'png',
                            'filename': f'Stress_History_Element_{selected_elem + 1 if selected_elem is not None else 0}',
                            'height': 600,
                            'width': 900,
                            'scale': 3
                        }
                    }
                )
            else:
                st.warning("No stress history records found.")

        st.markdown("---")
        col_tbl1, col_tbl2 = st.columns(2)
        with col_tbl1:
            with st.expander("📋 View Thermal Raw History Data"):
                render_html_table(df_thermal_raw)
        with col_tbl2:
            with st.expander("📋 View Stress Raw History Data"):
                render_html_table(df_stress_raw)

# ---------------------------------------------------------
# PAGE 3: PLASTICITY, DEFORMATION & STRESS REPORT
# ---------------------------------------------------------
elif page == "3. Plasticity, Deformation & Stress Report":
    st.title("📊 Plasticity, Deformation & Stress Report")
    st.markdown('<p style="font-size: 1.25rem; color: #333; margin-top: -10px; margin-bottom: 20px;">Top 10 critical nodes/elements report at the end of welding time based on selected criterion</p>', unsafe_allow_html=True)

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
                            "ID": str(elements_data[idx]["element_id"]),
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
                            "ID": str(node_id),
                            "Value (MPa)": f"{val:.2f}",
                            "X (m)": f"{float(row.get('x', 0)):.4f}",
                            "Y (m)": f"{float(row.get('y', 0)):.4f}",
                            "Z (m)": f"{float(row.get('z', 0)):.4f}"
                        })
                df_report = pd.DataFrame(report_rows)
                render_html_table(df_report)
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
                        "ID": str(int(node_id)),
                        "Displacement (m)": f"{val:.6f}",
                        "Displacement (mm)": f"{val * 1000:.2f}",
                        "X (m)": f"{float(row.get('x', 0)):.4f}",
                        "Y (m)": f"{float(row.get('y', 0)):.4f}",
                        "Z (m)": f"{float(row.get('z', 0)):.4f}"
                    })
                df_report = pd.DataFrame(report_rows)
                render_html_table(df_report)
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
                            "ID": str(elements_data[idx]["element_id"]),
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
                            "ID": str(int(node_id)),
                            "PEEQ Value": f"{val:.4f}",
                            "X (m)": f"{float(row.get('x', 0)):.4f}",
                            "Y (m)": f"{float(row.get('y', 0)):.4f}",
                            "Z (m)": f"{float(row.get('z', 0)):.4f}"
                        })
                df_report = pd.DataFrame(report_rows)
                render_html_table(df_report)
            else:
                st.warning("No plastic strain data available.")

# ---------------------------------------------------------
# PAGE 4: SIMULATION TIME-STEP ARCHIVE (SORTED BY TEMPERATURE)
# ---------------------------------------------------------
elif page == "4. Simulation Time-Step Archive (dt = 0.02s)":
    st.title("⏱️ Simulation Time-Step Archive")
    st.markdown('<p style="font-size: 1.25rem; color: #333; margin-top: -10px; margin-bottom: 20px;">Discrete query interface for high-resolution 50 Hz thermomechanical frames (Sorted by Temperature)</p>', unsafe_allow_html=True)

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
            st.subheader(f"🔥 Top 10 Hottest Nodes Summary at t = {selected_time:.3f} s")

            if len(temps) > 0:
                sorted_temp_indices = np.argsort(temps)[::-1]
                top_indices = sorted_temp_indices[:10]

                report_rows = []
                for rank, node_id in enumerate(top_indices, 1):
                    t_val = float(temps[node_id]) if node_id < len(temps) else 0.0
                    s_val = float(stresses[node_id]) if node_id < len(stresses) else 0.0
                    d_val = float(displacements[node_id, 2]) if node_id < len(displacements) else 0.0
                    row = df_mesh.iloc[node_id] if node_id < n_nodes else df_mesh.iloc[0]

                    report_rows.append({
                        "Rank": rank,
                        "Node ID": str(int(node_id)),
                        "Temperature (°C)": f"{t_val:.2f}",
                        "Stress (MPa)": f"{s_val:.2f}",
                        "Displacement Z (m)": f"{d_val:.6f}",
                        "X (m)": f"{float(row.get('x', 0)):.4f}",
                        "Y (m)": f"{float(row.get('y', 0)):.4f}",
                        "Z (m)": f"{float(row.get('z', 0)):.4f}"
                    })

                df_summary = pd.DataFrame(report_rows)
                render_html_table(df_summary)
            else:
                st.warning("No temperature data available for this frame.")
    else:
        st.warning("No time-step frames or geometry found in database collection.")

# ---------------------------------------------------------
# PAGE 5: CLUSTER HEALTH & INFRASTRUCTURE MONITOR
# ---------------------------------------------------------
elif page == "5. Cluster Health & Pipeline Infrastructure":
    st.title("🖥️ Cluster Health & Infrastructure Monitor")
    st.markdown('<p style="font-size: 1.25rem; color: #333; margin-top: -10px; margin-bottom: 20px;">Real-time operational health inspection of Docker container services</p>', unsafe_allow_html=True)

    try:
        docker_client = docker.from_env()
        containers = [c for c in docker_client.containers.list(all=True) if any(k in c.name for k in ["telemetry_", "welding_twin_"])]        

        container_data = []
        running_count = 0
        total_count = len(containers)

        for c in containers:
            status = c.status
            if status == "running":
                running_count += 1
                status_display = '<span style="background-color: #e6f4ea; color: #137333; font-weight: 600; padding: 3px 8px; border-radius: 4px;">RUNNING</span>'
            else:
                status_display = f'<span style="background-color: #fce8e6; color: #c5221f; font-weight: 600; padding: 3px 8px; border-radius: 4px;">{status.upper()}</span>'

            ports = ", ".join([f"{k}->{v[0]['HostPort']}" for k, v in c.ports.items() if v]) if c.ports else "None"
            container_data.append({
                "Container Name": c.name,
                "Status": status_display,
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

        render_html_table(df_containers)

    except Exception as e:
        st.error(f"Unable to connect to Docker daemon: {e}")
        st.info("Ensure `/var/run/docker.sock:/var/run/docker.sock` is mounted in `docker-compose.yml`.")