import streamlit as st
import requests
import time

# --- Configuration ---
st.set_page_config(page_title="Raft Cluster Dashboard", layout="wide")
st.title("🌊 Raft Distributed KV Store")

# Define our cluster nodes
NODES = {
    "Node 1": "http://localhost:8081",
    "Node 2": "http://localhost:8082",
    "Node 3": "http://localhost:8083"
}

# --- Sidebar: Cluster Health Monitor ---
st.sidebar.header("📡 Cluster Health")
active_node = None

for name, url in NODES.items():
    try:
        # Ping the node to see if it's alive (using a dummy get request)
        res = requests.get(f"{url}/get?key=health_check", timeout=1)
        st.sidebar.success(f"{name} is ONLINE (Port {url.split(':')[-1]})")
        
        # Default to the first responding node for our write/read operations
        if not active_node:
            active_node = url
    except requests.exceptions.RequestException:
        st.sidebar.error(f"{name} is OFFLINE")

st.sidebar.markdown("---")
st.sidebar.info("💡 **Pro Tip:** Go to your VS Code terminal and kill one of the nodes (Ctrl+C). Refresh this app to see the cluster health change, while the data remains safe!")

# --- Main Interface ---
if active_node:
    col1, col2 = st.columns(2)

    # WRITE DATA SECTION
    with col1:
        st.subheader("✍️ Write Data to Cluster")
        with st.form("write_form"):
            key_input = st.text_input("Key", placeholder="e.g., sensor_01")
            val_input = st.text_input("Value", placeholder="e.g., active")
            submit_write = st.form_submit_button("Store Data")

            if submit_write and key_input and val_input:
                try:
                    payload = {"key": key_input, "value": val_input}
                    response = requests.post(f"{active_node}/put", json=payload)
                    if response.status_code == 200:
                        st.success(f"Successfully stored '{key_input}': '{val_input}'")
                    else:
                        st.error("Failed to store data.")
                except Exception as e:
                    st.error(f"Network error: {e}")

    # READ DATA SECTION
    with col2:
        st.subheader("🔍 Read Data from Cluster")
        with st.form("read_form"):
            search_key = st.text_input("Search Key", placeholder="e.g., sensor_01")
            submit_read = st.form_submit_button("Fetch Data")

            if submit_read and search_key:
                try:
                    response = requests.get(f"{active_node}/get?key={search_key}")
                    if response.status_code == 200:
                        st.info(f"**Value:** {response.text.strip()}")
                    else:
                        st.warning("Key not found.")
                except Exception as e:
                    st.error(f"Network error: {e}")
else:
    st.error("🚨 CRITICAL: Entire cluster is offline. Start your Go nodes!")