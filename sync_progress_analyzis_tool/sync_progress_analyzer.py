import subprocess
import sys

def upgrade_dependencies():
    """Attempts to upgrade the core dependencies of the application at startup."""
    print("--- Checking for dashboard dependency updates ---")
    try:
        # Using a single call to pip is more efficient. Add scipy for KDE plot.
        subprocess.run(
            [sys.executable, "-m", "pip", "install", "--upgrade", "dash", "dash-bootstrap-components", "pandas", "plotly", "scipy"],
            check=True, capture_output=True, text=True, timeout=300 # 5 minute timeout
        )
        print("Dependencies are up to date.")
    except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired) as e:
        print(f"Could not automatically upgrade dependencies. The application will run with the current versions.")
        print(f"Details: {e.stderr if hasattr(e, 'stderr') else e}")
    print("--- Update check finished ---")

# Run the upgrade check at startup
upgrade_dependencies()

import pandas as pd
import dash
from dash import dcc, html
from dash.dependencies import Input, Output, State
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import base64
import io
from datetime import timedelta
import datetime
import numpy as np
from scipy.stats import gaussian_kde
from plotly.subplots import make_subplots
import os
import webbrowser
import traceback
import re

# --- Monkey-patching for older Dash versions ---
# This is a workaround for older Dash versions where 'loading_state' might not be
# a registered property on all components, causing validation errors.
# This dynamically adds the property to the component's list of known props.
# It's safe to run even on newer versions where the property already exists.
try:
    # The property name list can be either `_prop_names` or `prop_names`
    # depending on the Dash version. We try both.
    prop_names_attr = None
    if hasattr(dcc.Graph, '_prop_names'):
        prop_names_attr = '_prop_names'
    elif hasattr(dcc.Graph, 'prop_names'):
        prop_names_attr = 'prop_names'

    if prop_names_attr:
        prop_names = getattr(dcc.Graph, prop_names_attr)
        if 'loading_state' not in prop_names:
            prop_names.append('loading_state')
            print("Info: Monkey-patched dcc.Graph to support 'loading_state'.")
except Exception as e:
    print(f"Warning: Could not monkey-patch dcc.Graph for loading_state: {e}. The loading overlay might not work if you are on an old version of Dash.")

# --- SCRIPT DIRECTORY ---
# Use the real path to resolve any symlinks and get the directory of the script
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

# --- CSS and Asset Management ---
CUSTOM_CSS = """
/*
  HIGH-SPECIFICITY DROPDOWN OVERRIDE
  Targets dropdowns by ID to ensure these styles have the highest priority.
  Includes both modern (Dash >= 2.0) and older (Dash < 2.0) selectors.
*/

/* --- Dropdown Control (the main input box) --- */
[id*='"type":"start-block-dropdown"'] .Select-control,
[id*='"type":"end-block-dropdown"'] .Select-control,
[id*='"type":"start-block-dropdown"'] .Select__control,
[id*='"type":"end-block-dropdown"'] .Select__control {
    background-color: transparent !important;
    border: none !important;
    box-shadow: none !important;
}

/* --- Text inside the dropdown (selected value, input) --- */
[id*='"type":"start-block-dropdown"'] .Select-value, 
[id*='"type":"end-block-dropdown"'] .Select-value,
[id*='"type":"start-block-dropdown"'] .Select-value-label, 
[id*='"type":"end-block-dropdown"'] .Select-value-label,
[id*='"type":"start-block-dropdown"'] .Select-input > input,
[id*='"type":"end-block-dropdown"'] .Select-input > input,
[id*='"type":"start-block-dropdown"'] .Select__single-value,
[id*='"type":"end-block-dropdown"'] .Select__single-value,
[id*='"type":"start-block-dropdown"'] .Select__input-container,
[id*='"type":"end-block-dropdown"'] .Select__input-container {
    color: var(--bs-body-color) !important;
}

/* --- Right-align selected value in dropdowns --- */
/* This aligns the selected value and the input text to the right for consistency. */
[id*='"type":"start-block-dropdown"'] .Select-value,
[id*='"type":"end-block-dropdown"'] .Select-value,
[id*='"type":"start-block-dropdown"'] .Select__value-container,
[id*='"type":"end-block-dropdown"'] .Select__value-container {
    text-align: right;
    margin-right: 15px; /* Add margin to create space between the text container and the arrow */
}

/* --- The dropdown menu container --- */
[id*='"type":"start-block-dropdown"'] .Select-menu-outer,
[id*='"type":"end-block-dropdown"'] .Select-menu-outer,
[id*='"type":"start-block-dropdown"'] .Select__menu,
[id*='"type":"end-block-dropdown"'] .Select__menu {
    background-color: var(--bs-body-bg) !important;
    border: 1px solid var(--bs-border-color) !important;
}

/* --- Individual options in the dropdown --- */
[id*='"type":"start-block-dropdown"'] .Select-option,
[id*='"type":"end-block-dropdown"'] .Select-option,
[id*='"type":"start-block-dropdown"'] .Select__option,
[id*='"type":"end-block-dropdown"'] .Select__option {
    background-color: var(--bs-body-bg) !important;
    color: var(--bs-body-color) !important;
}

/* --- Focused option (hover) --- */
[id*='"type":"start-block-dropdown"'] .Select-option.is-focused,
[id*='"type":"end-block-dropdown"'] .Select-option.is-focused,
[id*='"type":"start-block-dropdown"'] .Select__option--is-focused,
[id*='"type":"end-block-dropdown"'] .Select__option--is-focused {
    background-color: var(--bs-primary) !important;
    color: white !important;
}

/* --- Selected option --- */
[id*='"type":"start-block-dropdown"'] .Select-option.is-selected,
[id*='"type":"end-block-dropdown"'] .Select-option.is-selected,
[id*='"type":"start-block-dropdown"'] .Select__option--is-selected,
[id*='"type":"end-block-dropdown"'] .Select__option--is-selected {
    background-color: var(--bs-secondary-bg) !important;
    color: var(--bs-body-color) !important; /* Ensure text is visible on selection */
}

/* --- Placeholder text --- */
[id*='"type":"start-block-dropdown"'] .Select--single > .Select-control .Select-placeholder,
[id*='"type":"end-block-dropdown"'] .Select--single > .Select-control .Select-placeholder,
[id*='"type":"start-block-dropdown"'] .Select__placeholder,
[id*='"type":"end-block-dropdown"'] .Select__placeholder {
    color: var(--bs-secondary-color) !important;
    text-align: left; /* Override right-alignment for placeholder text */
}

/* --- Arrow and Separator --- */
[id*='"type":"start-block-dropdown"'] .Select-arrow,
[id*='"type":"end-block-dropdown"'] .Select-arrow {
    border-color: var(--bs-body-color) transparent transparent !important;
}

[id*='"type":"start-block-dropdown"'] .Select__indicator-separator,
[id*='"type":"end-block-dropdown"'] .Select__indicator-separator {
    background-color: var(--bs-border-color) !important;
}

[id*='"type":"start-block-dropdown"'] .Select__indicator,
[id*='"type":"end-block-dropdown"'] .Select__indicator,
[id*='"type":"start-block-dropdown"'] .Select__dropdown-indicator,
[id*='"type":"end-block-dropdown"'] .Select__dropdown-indicator {
    color: var(--bs-secondary-color) !important;
}

/* --- Metadata Input Fields (New Property / Value) --- */
/* This ensures the text inputs in the metadata cards follow the theme. */
input[id*='"type":"metadata-key-input"'],
input[id*='"type":"metadata-value-input"'] {
#original-metadata-display .form-control,
#compare-metadata-display .form-control {
    background-color: var(--bs-body-bg) !important;
    color: var(--bs-body-color) !important;
    border: 1px solid var(--bs-border-color) !important;
}

input[id*='"type":"metadata-key-input"']::placeholder,
input[id*='"type":"metadata-value-input"']::placeholder {
#original-metadata-display .form-control::placeholder,
#compare-metadata-display .form-control::placeholder {
    color: var(--bs-secondary-color) !important;
}

/* --- Custom Checklist (for Save Filtered Range) --- */
/* This ensures the checkbox and its label follow the theme. */
.custom-checklist label {
    color: var(--bs-body-color) !important;
    display: inline-flex; /* Align label and custom checkbox */
    align-items: center;
    cursor: pointer;
}

.custom-checklist input[type="checkbox"] {
    /* Modern way to style the checkbox tick and border */
    accent-color: var(--bs-primary);
    /* Fallback for older browsers - basic theming */
    -webkit-appearance: none;
    -moz-appearance: none;
    appearance: none;
    
    width: 1.15em;
    height: 1.15em;
    
    border: 1px solid var(--bs-border-color);
    border-radius: 0.25em;
    background-color: var(--bs-body-bg);
    border: 1px solid var(--bs-border-color);
    
    display: inline-block;
    vertical-align: middle;
    position: relative;
    cursor: pointer;
    margin-right: 0.3em;
}

.custom-checklist input[type="checkbox"]:checked {
    background-color: var(--bs-primary);
    border-color: var(--bs-primary);
}

/* The checkmark */
.custom-checklist input[type="checkbox"]:checked::before {
    content: '✔';
    position: absolute;
    color: white;
    font-size: 0.9em;
    font-weight: bold;
    top: 50%;
    left: 50%;
    transform: translate(-50%, -50%);
    line-height: 1;
}
"""

def setup_directories():
    """Creates necessary directories if they don't exist."""
    # Assets directory for CSS
    assets_dir = os.path.join(SCRIPT_DIR, "assets")
    if not os.path.isdir(assets_dir):
        os.makedirs(assets_dir)
    with open(os.path.join(assets_dir, "custom_styles.css"), "w", encoding="utf-8") as f:
        f.write(CUSTOM_CSS)

    # Measurements directory for CSV files
    measurements_dir = os.path.join(SCRIPT_DIR, "measurements")
    if not os.path.isdir(measurements_dir):
        os.makedirs(measurements_dir)
        print(f"Info: Created 'measurements' directory at: {measurements_dir}")

# --- Helper Functions ---
def find_header_row(lines):
    """Finds the index of the header row in a list of lines by looking for key columns."""
    for i, line in enumerate(lines):
        # A good heuristic for the header is the presence of 'Block_height' and multiple semicolons
        if ('Block_height' in line or 'Block_timestamp' in line) and line.count(';') >= 2:
            return i
    return 0 # Fallback to the first line if no specific header is found

def extract_metadata(lines):
    """Extracts key-value metadata from the initial lines of the CSV."""
    metadata = {}
    in_metadata_section = False
    for line in lines:
        line = line.strip()
        if not in_metadata_section and 'Property;Value' in line:
            in_metadata_section = True
            continue
        if not in_metadata_section:
            continue
        if 'Block_height' in line or 'Block_timestamp' in line or line == ';;':
            break
        if ';' in line:
            parts = line.strip().split(';', 1)
            if len(parts) == 2 and parts[0] and parts[1]:
                metadata[parts[0]] = parts[1]
    return metadata

def load_csv_from_path(filepath):
    """Reads a CSV file from a given path and returns data for the store or an error feedback."""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        header_row = find_header_row(lines)
        metadata = extract_metadata(lines)
        df = pd.read_csv(io.StringIO("".join(lines[header_row:])), sep=';')
        df.columns = df.columns.str.strip()

        required_cols = ['Block_height', 'Accumulated_sync_in_progress_time[s]']
        if not all(col in df.columns for col in required_cols):
            missing_cols = [col for col in required_cols if col not in df.columns]
            raise ValueError(f"The file is missing essential columns: {', '.join(missing_cols)}.")

        store_data = {'filename': filepath, 'data': df.to_json(date_format='iso', orient='split'), 'metadata': metadata}
        feedback = {'title': 'File Reloaded', 'body': f"Successfully reloaded '{os.path.basename(filepath)}'."}
        return store_data, feedback
    except Exception as e:
        print(f"Error parsing file from path: {filepath}. Error: {e}")
        error_message = f"Failed to reload '{os.path.basename(filepath)}'.\n\nError: {e}"
        feedback = {'title': 'Reload Failed', 'body': error_message}
        return None, feedback

initial_metadata = {}
initial_csv_path = os.path.join(SCRIPT_DIR, "measurements", "sync_progress.csv")
try:
    with open(initial_csv_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    header_row = find_header_row(lines)
    initial_metadata = extract_metadata(lines)
    # Pass only the relevant lines to pandas, starting from the header
    df_progress = pd.read_csv(io.StringIO("".join(lines[header_row:])), sep=";")
    df_progress.columns = df_progress.columns.str.strip()
except FileNotFoundError:
    df_progress = pd.DataFrame()
    print(f"Info: {initial_csv_path} not found. Please upload a file or place it in the 'measurements' directory to begin analysis.")
except Exception as e:
    df_progress = pd.DataFrame()
    print(f"Error reading initial {initial_csv_path}: {e}")

# --- Helper Function ---
def format_seconds(seconds):
    """Formats seconds into a human-readable D-H-M-S string."""
    if pd.isna(seconds):
        return "N/A"
    return str(timedelta(seconds=int(seconds)))

# --- Tooltip Content ---
tooltip_texts = {
    'Original File': {
        'title': 'Original sync_progress.csv',
        'body': "This is the primary data file for analysis. It should be a CSV file containing synchronization progress data, typically named `sync_progress.csv`. The data from this file will be used to generate the main graphs and metrics. You can either drag and drop the file here or click to select it from your computer."
    },
    'Comparison File': {
        'title': 'Comparison sync_progress.csv',
        'body': "This is an optional secondary data file used for comparison. If you provide a second `sync_progress.csv` file here, the application will display its data alongside the original data, allowing for a side-by-side performance comparison. This is useful for evaluating the impact of changes in node configuration, hardware, or network conditions."
    },
    # System Info Tooltips
    'Signum Version': {
        'title': 'Signum Version',
        'body': "The version of the Signum-node software that generated the log file."
    },
    'Hostname': {
        'title': 'Hostname',
        'body': "The hostname of the machine running the Signum-node."
    },
    'OS Name': {
        'title': 'OS Name',
        'body': "The name of the operating system (e.g., Windows 11, Linux)."
    },
    'OS Version': {
        'title': 'OS Version',
        'body': "The version of the operating system."
    },
    'OS Architecture': {
        'title': 'OS Architecture',
        'body': "The architecture of the operating system (e.g., amd64, aarch64)."
    },
    'Java Version': {
        'title': 'Java Version',
        'body': "The version of the Java Runtime Environment (JRE) used to run the node."
    },
    'Available Processors': {
        'title': 'Available Processors',
        'body': "The number of CPU cores available to the Java Virtual Machine (JVM)."
    },
    'Max Memory (MB)': {
        'title': 'Max Memory (MB)',
        'body': "The maximum amount of memory (in Megabytes) that the JVM is configured to use (e.g., -Xmx)."
    },
    'Total RAM (MB)': {
        'title': 'Total RAM (MB)',
        'body': "The total physical RAM available on the machine, in Megabytes."
    },
    'Database Type': {
        'title': 'Database Type',
        'body': "The type of database used by the node (e.g., MariaDB, PostgreSQL, MySQL)."
    },
    'Database Version': {
        'title': 'Database Version',
        'body': "The version of the database software."
    },
    'Total Sync in Progress Time': {
        'title': 'Total Sync in Progress Time [s]',
        'body': "This metric represents the total duration the node has spent in an active synchronization state. The timer for this metric is active only when the node's block height is significantly behind the network's current height (typically more than 10 blocks). It provides a precise measure of the time required to catch up with the blockchain, excluding periods when the node is already fully synchronized or idle. This is a key performance indicator for evaluating the efficiency of the sync process under load."
    },
    'Total Blocks Synced': {
        'title': 'Total Blocks Synced',
        'body': "This value indicates the total number of blockchain blocks that have been downloaded, verified, and applied by the node during the measurement period. It is calculated as the difference between the final and initial block height. This metric, in conjunction with the sync time, is fundamental for calculating the overall synchronization speed."
    },
    'Overall Average Sync Speed [Blocks/sec]': {
        'title': 'Overall Average Sync Speed [Blocks/sec]',
        'body': "This is a high-level performance metric calculated by dividing the 'Total Blocks Synced' by the 'Total Sync in Progress Time' in seconds. It provides a single, averaged value representing the node's throughput over the entire synchronization process. While useful for a quick comparison, it can mask variations in performance that occur at different stages of syncing."
    },
    'Min Sync Speed [Blocks/sec sample]': {
        'title': 'Min Sync Speed [Blocks/sec Sampled]',
        'body': "This metric shows the minimum synchronization speed observed between any two consecutive data points in the sample. A very low or zero value can indicate periods of network latency, slow peer response, or high computational load on the node (e.g., during verification of blocks with many complex transactions), causing a temporary stall in progress."
    },
    'Q1 Sync Speed [Blocks/sec sample]': {
        'title': 'Q1/25th Percentile Sync Speed [Blocks/sec]',
        'body': "The 25th percentile (or first quartile, Q1) is the value below which 25% of the synchronization speed samples fall. It indicates the performance level that the node meets or exceeds 75% of the time. A higher Q1 value suggests that the node consistently maintains a good minimum performance level, with fewer periods of very low speed."
    },
    'Mean Sync Speed [Blocks/sec sample]': {
        'title': 'Mean Sync Speed [Blocks/sec Sampled]',
        'body': "This represents the statistical average (mean) of the 'Blocks/sec' values calculated for each interval between data points. Unlike the 'Overall Average', this metric is the average of individual speed samples, giving a more granular view of the typical performance, but it can be skewed by extremely high or low outliers."
    },
    'Median Sync Speed [Blocks/sec sample]': {
        'title': 'Median Sync Speed [Blocks/sec Sampled]',
        'body': "The median is the 50th percentile of the sampled 'Blocks/sec' values. It represents the middle value of the performance data, meaning half of the speed samples were higher and half were lower. The median is often a more robust indicator of central tendency than the mean, as it is less affected by extreme outliers or brief performance spikes/dips."
    },
    'Q3 Sync Speed [Blocks/sec sample]': {
        'title': 'Q3/75th Percentile Sync Speed [Blocks/sec]',
        'body': "The 75th percentile (or third quartile, Q3) is the value below which 75% of the synchronization speed samples fall. This metric highlights the performance level achieved during the faster sync periods. A high Q3 value indicates the node's capability to reach high speeds, but it should be considered alongside the median and mean to understand if this is a frequent or occasional event."
    },
    'Max Sync Speed [Blocks/sec sample]': {
        'title': 'Max Sync Speed [Blocks/sec Sampled]',
        'body': "This metric captures the peak synchronization speed achieved between any two consecutive data points. High maximum values typically occur when the node receives a burst of blocks from a fast peer with low latency, often during periods where the blocks being processed are less computationally intensive."
    },
    'Std Dev of Sync Speed [Blocks/sec sample]': {
        'title': 'Std Dev of Sync Speed [Blocks/sec Sampled]',
        'body': "The standard deviation measures the amount of variation or dispersion of the sampled 'Blocks/sec' values. A low standard deviation indicates that the sync speed was relatively consistent and stable. A high standard deviation suggests significant volatility in performance, with large fluctuations between fast and slow periods. This can be caused by inconsistent network conditions, varying peer quality, or changes in block complexity."
    },
    'Skewness of Sync Speed [Blocks/sec sample]': {
        'title': 'Skewness of Sync Speed [Blocks/sec Sampled]',
        'body': "Skewness is a measure of the asymmetry of the performance data distribution.\n\n- A value near 0 indicates a roughly symmetrical distribution, where performance fluctuates evenly around the mean.\n- A positive value (right-skewed) suggests that the dataset has a long tail of high-speed values. This means the sync speed is often clustered at lower values with occasional bursts of very high speed.\n- A negative value (left-skewed) indicates a long tail of low-speed values. This typically means the sync speed is consistently high but is occasionally dragged down by periods of slowness."
    },
    'Moving Average Window': {
        'title': 'Moving Average (MA) Window',
        'body': "This slider controls the size of the moving average window, measured in the number of data samples (blocks). The moving average is used to smooth out short-term fluctuations and highlight longer-term trends in the 'Blocks/sec' performance metric.\n\nA smaller window (e.g., 10) makes the average more responsive to recent changes, showing more detail but also more noise.\n\nA larger window (e.g., 500) provides a much smoother trend line, making it easier to see the overall performance trend but masking short-lived performance spikes or dips."
    },
    'Reset View': {
        'title': 'Reset View',
        'body': "This button resets the start and end block height filters to their default values, showing the entire available range of data from the loaded CSV file(s). It's useful for quickly returning to the full data view after zooming in on a specific section."
    },
    'Clear CSV': {
        'title': 'Clear CSV',
        'body': "This button filters the currently loaded CSV data in memory, keeping only the header, the first data row (block 0), and every 5000th block thereafter. This action does not affect the original file on disk but updates the current session's data, which can improve performance and make long-term trends easier to see. All metadata is preserved. To save the filtered data, use the 'Save' or 'Save As...' buttons."
    },
    'Start Block Height': {
        'title': 'Start Block Height',
        'body': "Select the starting block height for the analysis range. The graphs and metrics will be calculated for the data from this block onwards."
    },
    'End Block Height': {
        'title': 'End Block Height',
        'body': "Select the ending block height for the analysis range. The graphs and metrics will be calculated for the data up to this block."
    },
    'Add Metadata': {
        'title': 'Add Metadata',
        'body': "Add a new custom property and value to the metadata of this file. This information will be saved with the file when using 'Save' or 'Save As...'."
    },
    'Block Height': {
        'title': 'Block Height',
        'body': "The sequential number of a block in the blockchain. It represents a specific point in the history of the ledger. This table shows data sampled at various block heights."
    },
    'Sync Time [s]': {
        'title': 'Sync Time [seconds]',
        'body': "The total accumulated time in seconds that the node has spent in an active synchronization state up to this specific block height. This is the raw, unformatted value."
    },
    'Sync Time [Formatted]': {
        'title': 'Sync Time [Formatted]',
        'body': "A human-readable representation of the 'Sync Time [s]', formatted as Days-Hours:Minutes:Seconds. This makes it easier to understand longer synchronization durations."
    },
    'Sync Speed [Blocks/sec]': {
        'title': 'Sync Speed [Blocks/sec Instantaneous]',
        'body': "The instantaneous synchronization speed, calculated as the number of blocks processed since the last data point, divided by the time elapsed during that interval. This metric shows the node's performance at a specific moment and can fluctuate based on network conditions and block complexity."
    },
    'Save Filtered Range': {
        'title': 'Save Filtered Range',
        'body': "If checked, only the data within the currently selected block range (defined by the 'Start Block Height' and 'End Block Height' dropdowns) will be included in the saved CSV file. If unchecked, the entire dataset for the file will be saved, ignoring the block range filter."
    },
    'Save': {
        'title': 'Save',
        'body': "Saves the current state of the data (including any added or modified metadata and filtering) by overwriting the corresponding file in the `measurements/saved` directory. The filename will be kept consistent, without adding a new timestamp. Use this to update an existing saved file."
    },
    'Save As...': {
        'title': 'Save As...',
        'body': "Saves the current state of the data as a new CSV file in the `measurements/saved` directory. A timestamp and a unique sequence number will be appended to the filename to prevent overwriting previous saves. Use this to create a new version of the file while preserving the old one."
    }
,
    'Distribution Bins': {
        'title': 'Distribution Bins',
        'body': "This slider controls the number of bins (columns) used to create the histograms in the distribution chart. A smaller number of bins provides a more general overview of the data distribution, while a larger number reveals more detail but can also appear more noisy. Adjust this to find the best representation for your data."
    }
}

# --- Prepare initial data for the store if df_progress is loaded ---
initial_original_data = None
if not df_progress.empty:
    initial_original_data = {
        'filename': initial_csv_path,
        'data': df_progress.to_json(date_format='iso', orient='split'),
        'metadata': initial_metadata
    }

def filter_df_for_clearing(df):
    """Keeps header, first row, and every 5000th row."""
    if df.empty or 'Block_height' not in df.columns:
        return df

    # Condition: Block_height is a multiple of 5000 OR is 0
    condition = (df['Block_height'] % 5000 == 0) | (df['Block_height'] == 0)

    filtered_df = df[condition]

    # Sort by block height and reset index to maintain order and remove potential duplicates from concat
    return filtered_df.sort_values(by='Block_height').drop_duplicates(subset=['Block_height']).reset_index(drop=True)

def create_combined_summary_table(df_original, df_compare, title_original, title_compare):
    """Creates a Dash component with a combined summary table of sync metrics."""

    # If no data is present at all, return nothing.
    if df_original.empty and df_compare.empty:
        return None

    def get_stats_dict(df):
        """Helper to calculate stats for a single dataframe, returning both display and raw values."""
        if df.empty or 'Blocks_per_Second' not in df.columns or len(df) < 2:
            na_result = {'display': 'N/A', 'raw': None}
            return {
            'Total Sync in Progress Time [s]': na_result, 'Total Blocks Synced': na_result, 'Overall Average Sync Speed [Blocks/sec]': na_result,
            'Min Sync Speed [Blocks/sec sample]': na_result,
            'Q1 Sync Speed [Blocks/sec sample]': na_result,
            'Mean Sync Speed [Blocks/sec sample]': na_result,
            'Median Sync Speed [Blocks/sec sample]': na_result,
            'Q3 Sync Speed [Blocks/sec sample]': na_result,
            'Max Sync Speed [Blocks/sec sample]': na_result,
            'Std Dev of Sync Speed [Blocks/sec sample]': na_result,
            'Skewness of Sync Speed [Blocks/sec sample]': na_result
            }

        bps_series = df['Blocks_per_Second'].iloc[1:]
        if bps_series.empty:
            stats = pd.Series(index=['min', '25%', 'mean', '50%', '75%', 'max', 'std'], dtype=float).fillna(0)
            skewness = 0.0
        else:
            stats = bps_series.describe(percentiles=[.25, .75])
            skewness = bps_series.skew()

        # Correctly calculate duration for a slice of data
        total_sync_seconds = df['Accumulated_sync_in_progress_time[s]'].iloc[-1] - df['Accumulated_sync_in_progress_time[s]'].iloc[0]
        total_blocks_synced = df['Block_height'].iloc[-1] - df['Block_height'].iloc[0]
        overall_avg_bps = total_blocks_synced / total_sync_seconds if total_sync_seconds > 0 else 0.0

        return {
            'Total Sync in Progress Time [s]': {'display': f"{format_seconds(total_sync_seconds)} ({int(total_sync_seconds)}s)", 'raw': total_sync_seconds},
            'Total Blocks Synced': {'display': f"{total_blocks_synced:,}", 'raw': float(total_blocks_synced)},
            'Overall Average Sync Speed [Blocks/sec]': {'display': f"{overall_avg_bps:.2f}", 'raw': overall_avg_bps},
        'Min Sync Speed [Blocks/sec sample]': {'display': f"{stats.get('min', 0):.2f}", 'raw': stats.get('min', 0.0)},
        'Q1 Sync Speed [Blocks/sec sample]': {'display': f"{stats.get('25%', 0):.2f}", 'raw': stats.get('25%', 0.0)},
        'Mean Sync Speed [Blocks/sec sample]': {'display': f"{stats.get('mean', 0):.2f}", 'raw': stats.get('mean', 0.0)},
        'Median Sync Speed [Blocks/sec sample]': {'display': f"{stats.get('50%', 0):.2f}", 'raw': stats.get('50%', 0.0)},
        'Q3 Sync Speed [Blocks/sec sample]': {'display': f"{stats.get('75%', 0):.2f}", 'raw': stats.get('75%', 0.0)},
        'Max Sync Speed [Blocks/sec sample]': {'display': f"{stats.get('max', 0):.2f}", 'raw': stats.get('max', 0.0)},
        'Std Dev of Sync Speed [Blocks/sec sample]': {'display': f"{stats.get('std', 0):.2f}", 'raw': stats.get('std', 0.0)},
        'Skewness of Sync Speed [Blocks/sec sample]': {'display': f"{skewness:.2f}", 'raw': skewness if pd.notna(skewness) else 0.0}
        }

    has_original = not df_original.empty
    has_comparison = not df_compare.empty

    stats_original = get_stats_dict(df_original) if has_original else {}
    stats_compare = get_stats_dict(df_compare) if has_comparison else {}

    # Use a fixed list of metrics to ensure consistent order and display
    metric_names = [
        'Total Sync in Progress Time [s]', 'Total Blocks Synced', 'Overall Average Sync Speed [Blocks/sec]',
        'Min Sync Speed [Blocks/sec sample]', 'Q1 Sync Speed [Blocks/sec sample]',
        'Mean Sync Speed [Blocks/sec sample]', 'Median Sync Speed [Blocks/sec sample]',
        'Q3 Sync Speed [Blocks/sec sample]', 'Max Sync Speed [Blocks/sec sample]',
        'Std Dev of Sync Speed [Blocks/sec sample]', 'Skewness of Sync Speed [Blocks/sec sample]'
    ]

    header_cells = [html.Th("Metric")]
    if has_original:
        header_cells.append(html.Th(title_original, style={'wordBreak': 'break-all'}))
    if has_comparison:
        header_cells.append(html.Th(title_compare, style={'wordBreak': 'break-all'}))

    table_header = [html.Thead(html.Tr(header_cells))]
    
    # Define which metrics are better when higher
    higher_is_better = {
        'Total Sync in Progress Time [s]': False,
        'Total Blocks Synced': True,
        'Overall Average Sync Speed [Blocks/sec]': True,
        'Min Sync Speed [Blocks/sec sample]': True,
        'Q1 Sync Speed [Blocks/sec sample]': True,
        'Mean Sync Speed [Blocks/sec sample]': True,
        'Median Sync Speed [Blocks/sec sample]': True,
        'Q3 Sync Speed [Blocks/sec sample]': True,
        'Max Sync Speed [Blocks/sec sample]': True,
        'Std Dev of Sync Speed [Blocks/sec sample]': False,
        'Skewness of Sync Speed [Blocks/sec sample]': 'closer_to_zero',
    }

    table_body_rows = []
    for metric in metric_names:
        info = tooltip_texts.get(metric, {})
        title = info.get('title', metric)
        if metric in tooltip_texts:
            info_icon = html.Span([
                "\u00A0",  # Non-breaking space
                html.I(className="bi bi-info-circle-fill text-info align-middle", style={'fontSize': '1.1em'}),
            ],
                id={'type': 'info-icon', 'metric': metric}, # type: ignore
                n_clicks=0,
                style={'cursor': 'pointer'},
                title='Click for more info'
            )
            metric_cell = html.Td([title, info_icon])
        else:
            metric_cell = html.Td(title)
            
        # Original value cell
        row_cells = [metric_cell]
        if has_original:
            original_val_display = stats_original.get(metric, {}).get('display', 'N/A')
            original_cell_content = [original_val_display]

            if has_comparison:
                original_raw = stats_original.get(metric, {}).get('raw')
                compare_raw = stats_compare.get(metric, {}).get('raw')

                if original_raw is not None and compare_raw is not None:
                    diff = original_raw - compare_raw
                    color_class = ""
                    is_better = None
                    hib = higher_is_better.get(metric)

                    if diff != 0:
                        if hib == 'closer_to_zero':
                            if abs(original_raw) < abs(compare_raw): is_better = True
                            elif abs(original_raw) > abs(compare_raw): is_better = False
                        elif hib is True:
                            if diff > 0: is_better = True
                            if diff < 0: is_better = False
                        elif hib is False:
                            if diff < 0: is_better = True
                            if diff > 0: is_better = False

                    if is_better is True: color_class = "text-success"
                    elif is_better is False: color_class = "text-danger"

                    if color_class:
                        if metric == 'Total Sync in Progress Time [s]':
                            sign = "+" if diff > 0 else "-"
                            diff_str = f"{sign}{format_seconds(abs(diff))} [{sign}{int(abs(diff))}s]"
                        else:
                            diff_str = f"{diff:+.2f}" if not isinstance(diff, str) else diff
                        original_cell_content.append(html.Span(f" ({diff_str})", className=f"small {color_class} fw-bold"))

            row_cells.append(html.Td(original_cell_content))

        # Comparison value cell (with difference)
        if has_comparison:
            # Calculate and display difference
            original_raw = stats_original.get(metric, {}).get('raw')
            compare_raw = stats_compare.get(metric, {}).get('raw')
            compare_val_display = stats_compare.get(metric, {}).get('display', 'N/A')
            compare_cell_content = [compare_val_display]

            if original_raw is not None and compare_raw is not None:
                diff = compare_raw - original_raw
                
                # Determine color
                color_class = ""
                is_better = None
                hib = higher_is_better.get(metric)

                if diff != 0:
                    if hib == 'closer_to_zero':
                        if abs(compare_raw) < abs(original_raw):
                            is_better = True
                        elif abs(compare_raw) > abs(original_raw):
                            is_better = False
                    elif hib is True: # Higher is better
                        if diff > 0: is_better = True
                        if diff < 0: is_better = False
                    elif hib is False: # Lower is better
                        if diff < 0: is_better = True
                        if diff > 0: is_better = False
                
                if is_better is True:
                    color_class = "text-success" # Green
                elif is_better is False:
                    color_class = "text-danger" # Red
                
                # Format difference string
                if metric == 'Total Blocks Synced':
                    diff_str = f"{diff:+,}"
                elif metric == 'Total Sync in Progress Time [s]':
                    sign = "+" if diff > 0 else "-"
                    diff_str = f"{sign}{format_seconds(abs(diff))} [{sign}{int(abs(diff))}s]"
                else:
                    diff_str = f"{diff:+.2f}"

                if color_class:
                    compare_cell_content.append(html.Span(f" ({diff_str})", className=f"small {color_class} fw-bold"))

            row_cells.append(html.Td(compare_cell_content))

        table_body_rows.append(html.Tr(row_cells))

    table_body = [html.Tbody(table_body_rows)]

    return html.Div([
        html.H5("Metrics Summary", className="mt-4"),
        dbc.Table(table_header + table_body, striped=True, bordered=True, hover=True, className="mt-2", responsive=True)
    ])

def process_progress_df(df, filename=""):
    """Adds calculated columns to the progress dataframe. Handles both sync_progress and sync_measurement formats."""
    if df.empty:
        return df

    time_col_s = 'Accumulated_sync_in_progress_time[s]'
    time_col_ms = 'Accumulated_sync_in_progress_time[ms]'
    
    # Detect which type of file it is based on columns and prepare a consistent time column
    if time_col_s in df.columns:
        time_col = time_col_s
    elif time_col_ms in df.columns:
        # Convert ms to s for consistency
        df[time_col_s] = df[time_col_ms] / 1000
        time_col = time_col_s
    else:
        print(f"Warning: DataFrame from '{filename}' does not contain a recognized time column.")
        return df

    if 'Block_height' not in df.columns:
        print(f"Warning: DataFrame from '{filename}' does not contain 'Block_height' column.")
        return df

    df['SyncTime_Formatted'] = df[time_col].apply(format_seconds)
    delta_blocks = df['Block_height'].diff()
    delta_time = df[time_col].diff()
    df['Blocks_per_Second'] = (delta_blocks / delta_time.replace(0, pd.NA)).fillna(0)
    return df

# --- Moving Average Window Values ---
ma_windows = [10, 100, 200, 300, 400, 500]
ma_marks = {i: str(v) for i, v in enumerate(ma_windows)}

# --- Distribution Bins Values ---
dist_bins_values = [10, 100, 200, 300, 400, 500]
dist_bins_marks = {i: str(v) for i, v in enumerate(dist_bins_values)}


# --- Dash App Initialization ---
app = dash.Dash(
    __name__,
    external_stylesheets=[
        dbc.themes.BOOTSTRAP,
        "https://cdn.jsdelivr.net/npm/bootstrap-icons@1.10.3/font/bootstrap-icons.css"
    ],
    title="Sync Progress Reports"
)

# --- Common Styles ---
upload_style = {
    'width': '100%',
    'minHeight': '40px',  # Use minHeight instead of fixed height
    'padding': '5px 10px',  # Internal padding for content
    'display': 'flex',
    'justifyContent': 'center',
    'alignItems': 'center',
    'borderWidth': '1px',
    'borderStyle': 'dotted',
    'borderRadius': '5px',
    'textAlign': 'center', 'margin': '10px 0', 'fontSize': 'small', 'color': 'grey'
}

# --- Run directory setup at startup ---
setup_directories()

# --- App Layout ---
app.layout = html.Div([
    # Custom, centered loading overlay
    html.Div(id='loading-overlay', children=[
        dbc.Spinner(size="lg", color="primary", spinner_style={"width": "3rem", "height": "3rem"}),
        html.Br(),
        html.Div("Loading...", id='loading-overlay-message', style={'color': 'var(--bs-body-color)'} )
    ], className="bg-body bg-opacity-50", style={ # Use Bootstrap classes for theme-aware background
        'position': 'fixed', 'top': 0, 'left': 0, 'width': '100%', 'height': '100%',
        'display': 'none', 'justifyContent': 'center', 'alignItems': 'center',
        'flexDirection': 'column', 'zIndex': 10000,
    }),

    dcc.Store(id='loading-state-store', data={'loading': False, 'message': ''}),
    dcc.Store(id='reports-filepath-store'),
    html.Link(id="theme-stylesheet", rel="stylesheet"),
    dcc.Store(id='theme-store', storage_type='local'),
    dbc.Container([
    # Ghost graph components (no longer used for loading overlay)
    dcc.Graph(id='upload-callback-output', style={'display': 'none'}),
    dcc.Graph(id='clear-callback-output', style={'display': 'none'}),
    dcc.Graph(id='main-callback-output', style={'display': 'none'}),
    dcc.Graph(id='save-callback-output', style={'display': 'none'}),
    dcc.Store(id='original-data-store', data=initial_original_data),
    dcc.Store(id='compare-data-store'), # No initial data for comparison
    dcc.Store(id='action-feedback-store'), # For modal feedback
    dcc.Store(id='unsaved-changes-store', data={'Original': False, 'Comparison': False}),
    dcc.Store(id='reset-upload-store'), # To trigger clientside upload reset
    dcc.Store(id='html-content-store'), # For saving HTML content
        dbc.Row([
        dbc.Col(html.H1("Sync Progress Reports", className="mt-3 mb-4"), width="auto", className="me-auto"),
        dbc.Col([
            dbc.Button(html.I(className="bi bi-save"), id="save-button", color="secondary", className="me-3", title="Save Reports as HTML"),
            html.I(className="bi bi-sun-fill", style={'color': 'orange', 'fontSize': '1.2rem', 'verticalAlign': 'middle'}),
            dbc.Switch(id="theme-switch", value=True, className="d-inline-block mx-2"),
            html.I(className="bi bi-moon-stars-fill", style={'color': 'royalblue', 'fontSize': '1.2rem'}),
        ], width="auto", className="d-flex align-items-center mt-3")
    ], align="center"),

    dbc.Alert(
        "Original 'sync_progress.csv' not found in 'measurements' folder. Please place it there or upload a file to begin.",
        id="no-file-alert",
        color="warning",
        is_open=initial_original_data is None,
    ),

    dbc.Row([
        dbc.Col([
            html.Div([
                html.Div(dcc.Upload(
                    id='upload-original-progress',
                    children=html.Div(['Drag and Drop or ', html.A('Select Original sync_progress.csv')]),
                    style=upload_style,
                    multiple=False,
                ), style={'flexGrow': 1}),
                dbc.Button(html.I(className="bi bi-arrow-clockwise"), id="reload-original-button", color="primary", outline=True, className="ms-2", style={'display': 'none'}, title="Reload this file"),
                dbc.Button(html.I(className="bi bi-trash-fill"), id="discard-original-button", color="danger", outline=True, className="ms-2", style={'display': 'none'}, title="Discard this file"),
                html.Span([ # type: ignore
                    "\u00A0",  # Non-breaking space
                    html.I(className="bi bi-info-circle-fill text-info align-middle", style={'fontSize': '1.1em'}),
                ],
                    id={'type': 'info-icon', 'metric': 'Original File'}, # type: ignore
                    style={'cursor': 'pointer', 'marginLeft': '10px'},
                    title='Click for more info'
                ,
                n_clicks=0)
            ], style={'display': 'flex', 'alignItems': 'center'}, id='original-upload-container'),
            html.Div(id='original-metadata-display', className="mt-3")
        ]),
        dbc.Col([
            html.Div([
                html.Div(dcc.Upload(
                    id='upload-compare-progress',
                    children=html.Div(['Drag and Drop or ', html.A('Select Comparison sync_progress.csv')]),
                    style=upload_style, multiple=False
                ), style={'flexGrow': 1}),
                dbc.Button(html.I(className="bi bi-arrow-clockwise"), id="reload-compare-button", color="primary", outline=True, className="ms-2", style={'display': 'none'}, title="Reload this file"),
                dbc.Button(html.I(className="bi bi-trash-fill"), id="discard-compare-button", color="danger", outline=True, className="ms-2", style={'display': 'none'}, title="Discard this file"),
                html.Span([ "\u00A0", html.I(className="bi bi-info-circle-fill text-info align-middle", style={'fontSize': '1.1em'}), ], id={'type': 'info-icon', 'metric': 'Comparison File'}, style={'cursor': 'pointer', 'marginLeft': '10px'}, title='Click for more info', n_clicks=0), # type: ignore
            ], style={'display': 'flex', 'alignItems': 'center'}, id='compare-upload-container'),
            html.Div(id='compare-metadata-display', className="mt-3")
        ]),
    ]),
    html.Div([
        dcc.Graph(id="progress-graph", className="my-4"),
        dcc.Graph(id="blockheight-vs-speed-graph", className="my-4"),
        html.Div([
            html.Label("Moving Average Window:", className="me-2"),
            html.Span([html.I(className="bi bi-info-circle-fill text-info align-middle", style={'fontSize': '1.1em'})], id={'type': 'info-icon', 'metric': 'Moving Average Window'}, style={'cursor': 'pointer'}, title='Click for more info', n_clicks=0),
            html.Div(dcc.Slider(id="ma-window-slider-progress", min=0, max=len(ma_windows) - 1, value=1, marks=ma_marks, step=None), style={'width': '250px', 'marginLeft': '10px'}), # type: ignore
        ], id="ma-slider-container", style={'display': 'flex', 'alignItems': 'flex-start', 'marginTop': '20px'}),
        dcc.Graph(id="distribution-graph", className="my-4"),
        html.Div([
            html.Label("Distribution Bins:", className="me-2"),
            html.Span([html.I(className="bi bi-info-circle-fill text-info align-middle", style={'fontSize': '1.1em'})], id={'type': 'info-icon', 'metric': 'Distribution Bins'}, style={'cursor': 'pointer'}, title='Click for more info', n_clicks=0),
            html.Div(dcc.Slider(id="distribution-bins-slider", min=0, max=len(dist_bins_values) - 1, value=1, marks=dist_bins_marks, step=None), style={'width': '250px', 'marginLeft': '10px'}), # type: ignore
        ], id="distribution-bins-slider-container", style={'display': 'flex', 'alignItems': 'flex-start', 'marginTop': '20px'}),
        html.Div(id="total-time-display-container"),
        html.Hr(),
        html.H3("Raw Data View", className="mt-4"),
        html.Div([dbc.Switch(
                id='show-data-table-switch',
                label="Show Raw Data Table",
                value=True,
            )], id='show-data-table-switch-container'),
        html.Div(id='data-table-container'),
    ]),

    # Add Modal to layout
    dbc.Modal(
        [
            dbc.ModalHeader(dbc.ModalTitle(id="tooltip-modal-title")),
            dbc.ModalBody(id="tooltip-modal-body"),
        ], # type: ignore
        id="tooltip-modal",
        is_open=False,
        size="lg", # Larger modal for more text
    ),
    dbc.Modal(
        [
            dbc.ModalHeader(dbc.ModalTitle(id="action-feedback-modal-title")),
            dbc.ModalBody(id="action-feedback-modal-body"),
            dbc.ModalFooter([
                dbc.Button("Open Report", id="open-report-button", color="primary"),
                dbc.Button("Close", id="action-feedback-modal-close", className="ms-auto", n_clicks=0)
            ]),
        ],
        id="action-feedback-modal",
        is_open=False,
    )], fluid=True, id="main-container")
])


# --- New Callbacks for handling uploads and storing data ---
@app.callback(
    [Output('original-data-store', 'data', allow_duplicate=True),
     Output('action-feedback-store', 'data', allow_duplicate=True),
     Output('upload-callback-output', 'figure', allow_duplicate=True),
     Output('loading-state-store', 'data', allow_duplicate=True),
     Output('unsaved-changes-store', 'data', allow_duplicate=True)],
    [Input('upload-original-progress', 'contents'),
     Input('upload-original-progress', 'filename')],
    [State('unsaved-changes-store', 'data')],
    prevent_initial_call=True
)
def store_original_data(contents, filename, unsaved_data):
    if not contents:
        return dash.no_update, dash.no_update, dash.no_update, {'loading': False, 'message': ''}, dash.no_update
    # Set loading overlay ON
    loading_data = {'loading': True, 'message': 'Processing: sync_progress.csv'}
    content_type, content_string = contents.split(',')
    decoded = base64.b64decode(content_string)
    try:
        decoded_str = decoded.decode('utf-8')
        lines = decoded_str.splitlines(True) # Keep newlines
        header_row = find_header_row(lines)
        metadata = extract_metadata(lines)
        # Pass only the data part of the file to pandas
        df = pd.read_csv(io.StringIO("".join(lines[header_row:])), sep=';')
        df.columns = df.columns.str.strip() # Sanitize column names

        # Check for essential columns
        required_cols = ['Block_height', 'Accumulated_sync_in_progress_time[s]']
        if not all(col in df.columns for col in required_cols):
            missing_cols = [col for col in required_cols if col not in df.columns]
            raise ValueError(f"The uploaded file is missing essential columns: {', '.join(missing_cols)}. Please check the file format.")
        # Store absolute path if not already absolute (assume saved folder)
        if not os.path.isabs(filename):
            abs_path = os.path.join(SCRIPT_DIR, "measurements", "saved", filename)
        else:
            abs_path = filename
        store_data = {'filename': abs_path, 'data': df.to_json(date_format='iso', orient='split'), 'metadata': metadata}
        feedback = {'title': 'File Uploaded', 'body': f"Successfully loaded '{filename}'."}
        
        # A newly uploaded file is considered "saved".
        # We must preserve the state of the other file.
        new_unsaved_data = unsaved_data.copy()
        new_unsaved_data['Original'] = False

        # Set loading overlay OFF
        return store_data, feedback, None, {'loading': False, 'message': ''}, new_unsaved_data
    except Exception as e:
        print(f"Error parsing original uploaded file: {e}")
        error_message = f"Failed to load '{filename}'.\n\nError: {e}\n\nPlease ensure it is a valid sync_progress.csv file."
        feedback = {'title': 'Upload Failed', 'body': error_message}
        return None, feedback, {}, {'loading': False, 'message': ''}, dash.no_update

@app.callback(
    [Output('compare-data-store', 'data', allow_duplicate=True),
     Output('action-feedback-store', 'data', allow_duplicate=True),
     Output('upload-callback-output', 'figure', allow_duplicate=True),
     Output('loading-state-store', 'data', allow_duplicate=True),
     Output('unsaved-changes-store', 'data', allow_duplicate=True)],
    [Input('upload-compare-progress', 'contents'),
     Input('upload-compare-progress', 'filename')],
    [State('unsaved-changes-store', 'data')],
    prevent_initial_call=True
)
def store_compare_data(contents, filename, unsaved_data):
    if not contents:
        return dash.no_update, dash.no_update, dash.no_update, {'loading': False, 'message': ''}, dash.no_update
    # Set loading overlay ON
    loading_data = {'loading': True, 'message': 'Processing: comparison sync_progress.csv'}
    content_type, content_string = contents.split(',')
    decoded = base64.b64decode(content_string)
    try:
        decoded_str = decoded.decode('utf-8')
        lines = decoded_str.splitlines(True) # Keep newlines
        header_row = find_header_row(lines)
        metadata = extract_metadata(lines)
        # Pass only the data part of the file to pandas
        df = pd.read_csv(io.StringIO("".join(lines[header_row:])), sep=';')
        df.columns = df.columns.str.strip() # Sanitize column names

        # Check for essential columns
        required_cols = ['Block_height', 'Accumulated_sync_in_progress_time[s]']
        if not all(col in df.columns for col in required_cols):
            missing_cols = [col for col in required_cols if col not in df.columns]
            raise ValueError(f"The uploaded file is missing essential columns: {', '.join(missing_cols)}. Please check the file format.")
        # Store absolute path if not already absolute (assume saved folder)
        if not os.path.isabs(filename):
            abs_path = os.path.join(SCRIPT_DIR, "measurements", "saved", filename)
        else:
            abs_path = filename
        store_data = {'filename': abs_path, 'data': df.to_json(date_format='iso', orient='split'), 'metadata': metadata}
        feedback = {'title': 'File Uploaded', 'body': f"Successfully loaded '{filename}'."}
        
        # A newly uploaded file is considered "saved".
        # We must preserve the state of the other file.
        new_unsaved_data = unsaved_data.copy()
        new_unsaved_data['Comparison'] = False

        # Set loading overlay OFF
        return store_data, feedback, None, {'loading': False, 'message': ''}, new_unsaved_data
    except Exception as e:
        print(f"Error parsing original uploaded file: {e}")
        error_message = f"Failed to load '{filename}'.\n\nError: {e}\n\nPlease ensure it is a valid sync_progress.csv file."
        feedback = {'title': 'Upload Failed', 'body': error_message}
        return None, feedback, {}, {'loading': False, 'message': ''}, dash.no_update
# --- Loading overlay control callback ---
@app.callback(
    Output('loading-overlay', 'style', allow_duplicate=True),
    Output('loading-overlay-message', 'children', allow_duplicate=True),
    Input('loading-state-store', 'data'),
    prevent_initial_call=True
)
def update_loading_overlay(loading_data):
    if loading_data and loading_data.get('loading'):
        return {
            'position': 'fixed', 'top': 0, 'left': 0, 'width': '100%', 'height': '100%',
            'display': 'flex', 'justifyContent': 'center', 'alignItems': 'center', 'flexDirection': 'column',
            'zIndex': 10000
        }, loading_data.get('message', 'Loading...')
    else:
        return {
            'display': 'none', 'justifyContent': 'center', 'alignItems': 'center',
            'flexDirection': 'column', 'zIndex': 10000,
        }, ''

# --- New callback for the clear button ---
@app.callback(
    [Output('original-data-store', 'data', allow_duplicate=True),
     Output('compare-data-store', 'data', allow_duplicate=True),
     Output('action-feedback-store', 'data', allow_duplicate=True),
     Output('unsaved-changes-store', 'data', allow_duplicate=True),
     Output('clear-callback-output', 'figure')],
    Input({'type': 'clear-csv-button', 'prefix': dash.dependencies.ALL}, 'n_clicks'),
    [State('original-data-store', 'data'),
     State('compare-data-store', 'data'),
     State('unsaved-changes-store', 'data')],
    prevent_initial_call=True
)
def clear_csv_data(n_clicks_list, original_data, compare_data, unsaved_data):
    ctx = dash.callback_context
    if not ctx.triggered or not any(n_clicks_list):
        raise dash.exceptions.PreventUpdate

    triggered_id = ctx.triggered_id
    prefix = triggered_id.get('prefix') if isinstance(triggered_id, dict) else None

    feedback_messages = []
    new_original_data = dash.no_update
    new_compare_data = dash.no_update

    if prefix == 'Original':
        if original_data and 'data' in original_data:
            df_orig = pd.read_json(io.StringIO(original_data['data']), orient='split')
            rows_before = len(df_orig)
            df_orig_filtered = filter_df_for_clearing(df_orig)
            rows_after = len(df_orig_filtered)
            original_data['data'] = df_orig_filtered.to_json(date_format='iso', orient='split')
            unsaved_data['Original'] = True
            new_original_data = original_data
            feedback_messages.append(f"'{original_data.get('filename', 'Original file')}' filtered in memory from {rows_before:,} to {rows_after:,} rows.")
    elif prefix == 'Comparison':
        if compare_data and 'data' in compare_data:
            df_comp = pd.read_json(io.StringIO(compare_data['data']), orient='split')
            rows_before = len(df_comp)
            df_comp_filtered = filter_df_for_clearing(df_comp)
            rows_after = len(df_comp_filtered)
            compare_data['data'] = df_comp_filtered.to_json(date_format='iso', orient='split')
            unsaved_data['Comparison'] = True
            new_compare_data = compare_data
            feedback_messages.append(f"'{compare_data.get('filename', 'Comparison file')}' filtered in memory from {rows_before:,} to {rows_after:,} rows.")

    if not feedback_messages:
        feedback_body = "No data was loaded to clear."
    else:
        feedback_messages.append("\nClick 'Save' or 'Save As...' to persist these changes to a file.")
        feedback_body = "\n".join(feedback_messages)

    feedback_data = {'title': 'CSV Data Filtered', 'body': feedback_body}

    return new_original_data, new_compare_data, feedback_data, unsaved_data, {}

@app.callback(
    Output('upload-original-progress', 'children'),
    Input('original-data-store', 'data')
)
def update_original_upload_text(data):
    """Updates the text of the original upload component based on whether data is loaded."""
    if data and data.get('filename'):
        return html.Div(f"Selected Original file: {data['filename']}", style={'wordBreak': 'break-all'})
    return html.Div(['Drag and Drop or ', html.A('Select Original sync_progress.csv')])

@app.callback(
    Output('upload-compare-progress', 'children'),
    Input('compare-data-store', 'data')
)
def update_compare_upload_text(data):
    """Updates the text of the comparison upload component based on whether data is loaded."""
    if data and data.get('filename'):
        return html.Div(f"Selected Comparison file: {data['filename']}", style={'wordBreak': 'break-all'})
    return html.Div(['Drag and Drop or ', html.A('Select Comparison sync_progress.csv')])

@app.callback(
    [Output('original-data-store', 'data'),
     Output('upload-original-progress', 'contents', allow_duplicate=True),
     Output('unsaved-changes-store', 'data', allow_duplicate=True)],
    Input('discard-original-button', 'n_clicks'),
    State('unsaved-changes-store', 'data'),
    prevent_initial_call=True
)
def discard_original_data(n_clicks, unsaved_data):
    """Clears the original data store and resets the upload component when the discard button is clicked."""
    if not n_clicks:
        return dash.no_update, dash.no_update, dash.no_update
    
    new_unsaved_data = unsaved_data.copy()
    new_unsaved_data['Original'] = False
    # Setting store to None clears data, setting contents to None resets the Upload component
    return None, None, new_unsaved_data

@app.callback(
    [Output('compare-data-store', 'data', allow_duplicate=True),
     Output('upload-compare-progress', 'contents', allow_duplicate=True),
     Output('unsaved-changes-store', 'data', allow_duplicate=True)],
    Input('discard-compare-button', 'n_clicks'),
    State('unsaved-changes-store', 'data'),
    prevent_initial_call=True
)
def discard_compare_data(n_clicks, unsaved_data):
    """Clears the comparison data store and resets the upload component when the discard button is clicked."""
    if not n_clicks:
        return dash.no_update, dash.no_update, dash.no_update
    new_unsaved_data = unsaved_data.copy()
    new_unsaved_data['Comparison'] = False
    # Setting store to None clears data, setting contents to None resets the Upload component
    return None, None, new_unsaved_data

@app.callback(
    Output('discard-original-button', 'style'),
    Input('original-data-store', 'data')
)
def toggle_discard_original_button(data):
    """Shows or hides the discard button for the original file."""
    if data:
        return {'display': 'inline-block'}
    return {'display': 'none'}

@app.callback(
    Output('discard-compare-button', 'style'),
    Input('compare-data-store', 'data')
)
def toggle_discard_compare_button(data):
    """Shows or hides the discard button for the comparison file."""
    if data:
        return {'display': 'inline-block'}
    return {'display': 'none'}

@app.callback(
    Output('reload-original-button', 'style'),
    Input('original-data-store', 'data')
)
def toggle_reload_original_button(data):
    """Shows or hides the reload button for the original file."""
    if data and data.get('filename'):
        return {'display': 'inline-block'}
    return {'display': 'none'}

@app.callback(
    Output('reload-compare-button', 'style'),
    Input('compare-data-store', 'data')
)
def toggle_reload_compare_button(data):
    """Shows or hides the reload button for the comparison file."""
    if data and data.get('filename'):
        return {'display': 'inline-block'}
    return {'display': 'none'}

@app.callback(
    [Output('original-data-store', 'data', allow_duplicate=True),
     Output('action-feedback-store', 'data', allow_duplicate=True),
     Output('unsaved-changes-store', 'data', allow_duplicate=True)],
    Input('reload-original-button', 'n_clicks'),
    [State('original-data-store', 'data'),
     State('unsaved-changes-store', 'data')],
    prevent_initial_call=True
)
def reload_original_data(n_clicks, store_data, unsaved_data):
    if not n_clicks or not store_data or not store_data.get('filename'):
        raise dash.exceptions.PreventUpdate
    
    filepath_in_store = store_data['filename']
    
    if not os.path.isabs(filepath_in_store):
        filepath = os.path.join(SCRIPT_DIR, "measurements", filepath_in_store)
    else:
        filepath = filepath_in_store

    if not os.path.exists(filepath):
        feedback = {'title': 'Reload Failed', 'body': f"File not found at expected path: {filepath}"}
        return dash.no_update, feedback, dash.no_update

    new_store_data, feedback = load_csv_from_path(filepath)
    
    if new_store_data:
        new_store_data['filename'] = filepath
        new_unsaved_data = unsaved_data.copy()
        new_unsaved_data['Original'] = False
        return new_store_data, feedback, new_unsaved_data
    else:
        return dash.no_update, feedback, dash.no_update

@app.callback(
    [Output('compare-data-store', 'data', allow_duplicate=True),
     Output('action-feedback-store', 'data', allow_duplicate=True),
     Output('unsaved-changes-store', 'data', allow_duplicate=True)],
    Input('reload-compare-button', 'n_clicks'),
    State('compare-data-store', 'data'),
    prevent_initial_call=True
)
def reload_compare_data(n_clicks, store_data):
    if not n_clicks or not store_data or not store_data.get('filename'):
        raise dash.exceptions.PreventUpdate
    
    filepath_in_store = store_data['filename']
    
    if not os.path.isabs(filepath_in_store):
        filepath = os.path.join(SCRIPT_DIR, "measurements", filepath_in_store)
    else:
        filepath = filepath_in_store

    if not os.path.exists(filepath):
        feedback = {'title': 'Reload Failed', 'body': f"File not found at expected path: {filepath}"}
        return dash.no_update, feedback, dash.no_update

    new_store_data, feedback = load_csv_from_path(filepath)
    
    if new_store_data:
        new_store_data['filename'] = filepath
        # Since we are reloading, this file is now considered "saved"
        # We need to read the current state of unsaved changes and update it.
        current_unsaved = dash.callback_context.states['unsaved-changes-store.data']
        current_unsaved['Comparison'] = False
        return new_store_data, feedback, current_unsaved
    else:
        return dash.no_update, feedback, dash.no_update

# --- Callback to Update Progress Graph ---
@app.callback(
    [Output("progress-graph", "figure"),
     Output("blockheight-vs-speed-graph", "figure"),
     Output("distribution-graph", "figure"),
     Output("total-time-display-container", "children"),
     Output({'type': 'start-block-dropdown', 'prefix': dash.dependencies.ALL}, "options"),
     Output({'type': 'start-block-dropdown', 'prefix': dash.dependencies.ALL}, "value"),
     Output({'type': 'end-block-dropdown', 'prefix': dash.dependencies.ALL}, "options"),
     Output({'type': 'end-block-dropdown', 'prefix': dash.dependencies.ALL}, "value"),
     Output("main-callback-output", "figure"),
     Output("data-table-container", "children"),
     Output("data-table-container", "style"),
    ],
    [Input("ma-window-slider-progress", "value"), # 0
     Input("distribution-bins-slider", "value"),
     Input('original-data-store', 'data'),
     Input('compare-data-store', 'data'),
     Input({'type': 'start-block-dropdown', 'prefix': dash.dependencies.ALL}, 'value'),
     Input({'type': 'end-block-dropdown', 'prefix': dash.dependencies.ALL}, 'value'),
     Input({'type': 'reset-view-button', 'prefix': dash.dependencies.ALL}, 'n_clicks'),
     Input('show-data-table-switch', 'value'),
     Input('theme-store', 'data')],
)
def update_progress_graph_and_time(window_index, bins_index, original_data, compare_data,
                                   start_block_vals, end_block_vals, reset_clicks,
                                   show_data_table, theme):
    window = ma_windows[window_index]
    ctx = dash.callback_context

    # --- Define colors based on theme ---
    is_dark_theme = theme != 'light'
    original_bps_color = '#b86e1e' if is_dark_theme else '#FF8C00'  # Muted orange for dark theme (was 'darkorange')
    original_ma_color = '#e0943b' if is_dark_theme else 'orange'      # Muted Amber for dark theme
    hover_label_style = dict(bgcolor="rgba(255, 255, 255, 0.8)", font=dict(color='black'))
    
    background_style = {}
    if is_dark_theme:
        hover_label_style = dict(bgcolor="rgba(34, 37, 41, 0.9)", font=dict(color='white'))
        background_style = {'paper_bgcolor': 'rgba(0,0,0,0)', 'plot_bgcolor': 'rgba(0,0,0,0)'}

    # --- Map inputs to prefixes ---
    # The order of inputs is: ma-slider, original-data, compare-data, start-block-vals, end-block-vals, ...
    start_block_inputs = {} # start-block-dropdown values
    if ctx.inputs_list[4]:
        start_block_inputs = {item['id']['prefix']: item.get('value') for item in ctx.inputs_list[4]}

    end_block_inputs = {} # end-block-dropdown values
    if ctx.inputs_list[5]:
        end_block_inputs = {item['id']['prefix']: item.get('value') for item in ctx.inputs_list[5]}

 
    def create_header_with_tooltip(text, metric_id, style=None):
        if metric_id in tooltip_texts:
            info_icon = html.Span([ # type: ignore
                "\u00A0",  # Non-breaking space
                html.I(className="bi bi-info-circle-fill text-info align-middle", style={'fontSize': '1.1em'}),
            ],
                id={'type': 'info-icon', 'metric': metric_id},
                style={'cursor': 'pointer'},
                title='Click for more info'
            )
            return html.Th([text, info_icon], style=style)
        return html.Th(text, style=style)

    # Create figure with secondary y-axis
    fig = make_subplots(specs=[[{"secondary_y": True}]])


    # --- Load and process data from stores ---
    df_progress_local = pd.DataFrame()
    df_compare = pd.DataFrame()
    original_filename = "sync_progress.csv" # Default filename
    compare_filename = None

    # Load original data
    if original_data and 'data' in original_data:
        df_progress_local = pd.read_json(io.StringIO(original_data['data']), orient='split')
        original_filename = original_data.get('filename', original_filename)

    # Load comparison data
    if compare_data and 'data' in compare_data:
        df_compare = pd.read_json(io.StringIO(compare_data['data']), orient='split')
        compare_filename = compare_data.get('filename')

    # --- Handle Empty State ---
    if df_progress_local.empty and df_compare.empty:
        graph_template = 'plotly_dark' if theme != 'light' else 'plotly'
        empty_fig = go.Figure()
        empty_fig.update_layout(
            title_text='Upload a sync_progress.csv file to begin',
            template=graph_template,
            **background_style
        )
        summary_table = create_combined_summary_table(pd.DataFrame(), pd.DataFrame(), "Original", "Comparison")
        
        # Correctly form empty outputs for pattern-matching callbacks
        num_start_dds = len(ctx.outputs_grouping[4]) if 4 < len(ctx.outputs_grouping) else 0
        num_end_dds = len(ctx.outputs_grouping[6]) if 6 < len(ctx.outputs_grouping) else 0
        empty_start_opts = [[] for _ in range(num_start_dds)]
        empty_start_vals = [None for _ in range(num_start_dds)]
        empty_end_opts = [[] for _ in range(num_end_dds)]
        empty_end_vals = [None for _ in range(num_end_dds)]

        return empty_fig, empty_fig, empty_fig, summary_table, empty_start_opts, empty_start_vals, empty_end_opts, empty_end_vals, {}, None, {'display': 'none'}
    # --- Process full dataframes first to get Blocks_per_Second ---
    if not df_progress_local.empty:
        df_progress_local = process_progress_df(df_progress_local, original_filename)
    if not df_compare.empty:
        df_compare = process_progress_df(df_compare, compare_filename)

    # --- Prepare data for each card ---
    data_map = {
        'Original': {'df': df_progress_local, 'filename': original_filename, 'start_block': None, 'end_block': None, 'options': [], 'display_df': pd.DataFrame()},
        'Comparison': {'df': df_compare, 'filename': compare_filename, 'start_block': None, 'end_block': None, 'options': [], 'display_df': pd.DataFrame()}
    }

    triggered_id = ctx.triggered_id
    is_upload_or_clear = triggered_id in ['original-data-store', 'compare-data-store']
    is_reset = isinstance(triggered_id, dict) and triggered_id.get('type') == 'reset-view-button'

    # --- Calculate ranges and options for each file ---
    for prefix, info in data_map.items():
        df = info['df']
        if not df.empty:
            min_block = df['Block_height'].min()
            max_block = df['Block_height'].max()
            # Use unique and sorted values for dropdowns
            unique_heights = sorted(df['Block_height'].unique())
            info['options'] = [{'label': f"{int(h):,}", 'value': h} for h in unique_heights]
            
            current_start = start_block_inputs.get(prefix)
            current_end = end_block_inputs.get(prefix)

            # Determine start and end blocks based on context
            if is_reset and triggered_id.get('prefix') == prefix:
                info['start_block'] = min_block
                info['end_block'] = max_block
            elif is_upload_or_clear or current_start is None or current_end is None:
                info['start_block'] = min_block
                info['end_block'] = max_block
            else:
                info['start_block'] = current_start
                info['end_block'] = current_end

            # Swap if start > end
            if info['start_block'] is not None and info['end_block'] is not None and info['start_block'] > info['end_block']:
                info['start_block'], info['end_block'] = info['end_block'], info['start_block']
            
            # Filter data for display
            start, end = info['start_block'], info['end_block']
            if start is not None and end is not None:
                info['display_df'] = df[(df['Block_height'] >= start) & (df['Block_height'] <= end)].copy()
            else:
                info['display_df'] = df.copy()

    # --- Filter dataframes for display and metrics ---
    df_original_display = data_map['Original']['display_df']
    df_compare_display = data_map['Comparison']['display_df']

    # --- Plot Original Data ---
    if not df_original_display.empty:
        df_original_display['BPS_ma'] = df_original_display['Blocks_per_Second'].rolling(window=window, min_periods=1).mean()
        fig.add_trace(
            go.Scatter(
                x=df_original_display['Accumulated_sync_in_progress_time[s]'],
                y=df_original_display['Block_height'],
                name='Block Height (Original)',
                customdata=df_original_display[['SyncTime_Formatted']],
                hovertemplate=(
                    f'<b>File</b>: {original_filename}<br>' +
                    '<b>Block Height</b>: %{y}<br>' +
                    '<b>Sync Time</b>: %{customdata[0]} [%{x:.0f}s]' +
                    '<extra></extra>'
                )
            ),
            secondary_y=False,
        )
        fig.add_trace(
            go.Scatter(
                x=df_original_display['Accumulated_sync_in_progress_time[s]'],
                y=df_original_display['Blocks_per_Second'],
                name='Sync Speed (Original)',
                line=dict(color=original_bps_color, dash='dot', width=1), # type: ignore
                hovertemplate='<b>Sync Speed</b>: %{y:.2f} [Blocks/sec]<extra></extra>'
            ),
            secondary_y=True,
        )
        fig.add_trace(
            go.Scatter(
                x=df_original_display['Accumulated_sync_in_progress_time[s]'],
                y=df_original_display['BPS_ma'],
                name='Sync Speed (MA) (Original)',
                line=dict(color=original_ma_color, dash='solid'), # type: ignore
                hovertemplate='<b>Sync Speed (MA)</b>: %{y:.2f} [Blocks/sec]<extra></extra>'
            ),
            secondary_y=True,
        )

    # --- Plot Comparison Data if available ---
    if not df_compare_display.empty:
        df_compare_display['BPS_ma'] = df_compare_display['Blocks_per_Second'].rolling(window=window, min_periods=1).mean()
        fig.add_trace(
                go.Scatter(
                    x=df_compare_display['Accumulated_sync_in_progress_time[s]'],
                    y=df_compare_display['Block_height'],
                    name='Block Height (Comparison)',
                    line=dict(color='cyan'),
                    customdata=df_compare_display[['SyncTime_Formatted']],
                    hovertemplate=(
                        f'<b>File</b>: {compare_filename}<br>' +
                        '<b>Block Height</b>: %{y}<br>' +
                        '<b>Sync Time</b>: %{customdata[0]} [%{x:.0f}s]' +
                        '<extra></extra>'
                    )
                ),
                secondary_y=False,
            )
        fig.add_trace(
                go.Scatter(
                    x=df_compare_display['Accumulated_sync_in_progress_time[s]'],
                    y=df_compare_display['Blocks_per_Second'],
                    name='Sync Speed (Comparison)',
                    line=dict(color='fuchsia', dash='dot', width=1),
                    hovertemplate='<b>Sync Speed</b>: %{y:.2f} [Blocks/sec]<extra></extra>'
                ),
                secondary_y=True,
            )
        fig.add_trace(
                go.Scatter(
                    x=df_compare_display['Accumulated_sync_in_progress_time[s]'],
                    y=df_compare_display['BPS_ma'],
                    name='Sync Speed (MA) (Comparison)',
                    line=dict(color='magenta', dash='solid'),
                    hovertemplate='<b>Sync Speed (MA)</b>: %{y:.2f} [Blocks/sec]<extra></extra>'
                ),
                secondary_y=True,
            )
    # --- Define graph template before using it ---
    graph_template = 'plotly_dark' if theme != 'light' else 'plotly'
        
    # Create the second figure for Block Height vs. Sync Speed/Time
    fig2 = make_subplots(specs=[[{"secondary_y": True}]])

    # Plot Original Data for fig2
    if not df_original_display.empty:
        fig2.add_trace(
            go.Scatter(
                x=df_original_display['Block_height'],
                y=df_original_display['Accumulated_sync_in_progress_time[s]'],
                name='Sync Time (Original)',
                line=dict(color='#1f77b4'),
                customdata=df_original_display[['SyncTime_Formatted']],
                hovertemplate=(
                    f'<b>File</b>: {original_filename}<br>' +
                    '<b>Block Height</b>: %{x}<br>' +
                    '<b>Sync Time</b>: %{customdata[0]} [%{y:.0f}s]' +
                    '<extra></extra>'
                )
            ),
            secondary_y=False,
        )
        fig2.add_trace(
            go.Scatter(
                x=df_original_display['Block_height'],
                y=df_original_display['Blocks_per_Second'],
                name='Sync Speed (Original)',
                line=dict(color=original_bps_color, dash='dot', width=1),
                hovertemplate='<b>Sync Speed</b>: %{y:.2f} [Blocks/sec]<extra></extra>'
            ),
            secondary_y=True,
        )
        fig2.add_trace(
            go.Scatter(
                x=df_original_display['Block_height'],
                y=df_original_display['BPS_ma'],
                name='Sync Speed (MA) (Original)',
                line=dict(color=original_ma_color, dash='solid'),
                hovertemplate='<b>Sync Speed (MA)</b>: %{y:.2f} [Blocks/sec]<extra></extra>'
            ),
            secondary_y=True,
        )

    # Plot Comparison Data for fig2 if available
    if not df_compare_display.empty:
        fig2.add_trace(
            go.Scatter(
                x=df_compare_display['Block_height'],
                y=df_compare_display['Accumulated_sync_in_progress_time[s]'],
                name='Sync Time (Comparison)',
                line=dict(color='cyan'),
                customdata=df_compare_display[['SyncTime_Formatted']],
                hovertemplate=(
                    f'<b>File</b>: {compare_filename}<br>' +
                    '<b>Block Height</b>: %{x}<br>' +
                    '<b>Sync Time</b>: %{customdata[0]} [%{y:.0f}s]' +
                    '<extra></extra>'
                )
            ),
            secondary_y=False,
        )
        fig2.add_trace(
            go.Scatter(
                x=df_compare_display['Block_height'],
                y=df_compare_display['Blocks_per_Second'],
                name='Sync Speed (Comparison)',
                line=dict(color='fuchsia', dash='dot', width=1),
                hovertemplate='<b>Sync Speed</b>: %{y:.2f} [Blocks/sec]<extra></extra>'
            ),
            secondary_y=True,
        )
        fig2.add_trace(
            go.Scatter(
                x=df_compare_display['Block_height'],
                y=df_compare_display['BPS_ma'],
                name='Sync Speed (MA) (Comparison)',
                line=dict(color='magenta', dash='solid'),
                hovertemplate='<b>Sync Speed (MA)</b>: %{y:.2f} [Blocks/sec]<extra></extra>'
            ),
            secondary_y=True,
        )

    # Update layout for fig2
    fig2.update_layout(
        title_text=f'Sync Time and Sync Speed vs. Block Height (MA Window: {window})',
        height=600,
        hovermode='x unified', hoverlabel=hover_label_style,
        xaxis_title="<b>Block Height</b>",
        yaxis_title="<b>Sync in Progress Time [s]</b>",
        yaxis2_title="<b>Sync Speed [Blocks/sec]</b>",
        legend=dict(orientation="v", yanchor="top", y=-0.2, xanchor="left", x=0),
        template=graph_template,
        margin=dict(l=80, r=80, t=80, b=80),
        **background_style
    )

    # --- Distribution Graph ---
    bins = dist_bins_values[bins_index]
    fig3 = make_subplots(rows=1, cols=2, subplot_titles=('Sync Speed Distribution', 'Sync Time Delta Distribution'))

    def add_distribution_trace(fig, df, col, name, color, row, col_num, legend_name):
        if not df.empty and col in df.columns:
            data = df[col].dropna()
            if len(data) > 1:
                # Histogram
                fig.add_trace(go.Histogram(
                    x=data,
                    name=f'{name} (Histogram)',
                    marker_color=color,
                    opacity=0.6,
                    nbinsx=bins,
                    histnorm='probability density',
                    hovertemplate='<b>Range</b>: %{x}<br><b>Density</b>: %{y}<extra></extra>',
                    showlegend=True, # Show this trace in the legend
                    legend=legend_name
                ), row=row, col=col_num)

                # KDE Curve
                try:
                    kde = gaussian_kde(data)
                    x_range = np.linspace(data.min(), data.max(), 500)
                    kde_values = kde(x_range)
                    fig.add_trace(go.Scatter(
                        x=x_range,
                        y=kde_values,
                        mode='lines',
                        name=f'{name} (KDE)',
                        hovertemplate='<b>Value</b>: %{x:.2f}<br><b>Density</b>: %{y:.4f}<extra></extra>',
                        line=dict(color=color, width=2),
                        showlegend=True,
                        legend=legend_name
                    ), row=row, col=col_num)
                except Exception as e:
                    print(f"Could not generate KDE for {name} - {col}: {e}")

    # Sync Speed Distribution
    add_distribution_trace(fig3, df_original_display, 'Blocks_per_Second', 'Original', original_ma_color, 1, 1, 'legend1')
    if not df_compare_display.empty:
        add_distribution_trace(fig3, df_compare_display, 'Blocks_per_Second', 'Comparison', 'magenta', 1, 1, 'legend1')

    # Sync Time Delta Distribution
    if not df_original_display.empty:
        time_delta_orig = df_original_display['Accumulated_sync_in_progress_time[s]'].diff().dropna()
        if len(time_delta_orig) > 1:
            add_distribution_trace(fig3, pd.DataFrame({'TimeDelta': time_delta_orig}), 'TimeDelta', 'Original', original_ma_color, 1, 2, 'legend2')

    if not df_compare_display.empty:
        time_delta_comp = df_compare_display['Accumulated_sync_in_progress_time[s]'].diff().dropna()
        if len(time_delta_comp) > 1:
            add_distribution_trace(fig3, pd.DataFrame({'TimeDelta': time_delta_comp}), 'TimeDelta', 'Comparison', 'magenta', 1, 2, 'legend2')

    fig3.update_layout(
        title_text=f'Data Distribution (Bins: {bins})',
        height=500,
        bargap=0.1,
        template=graph_template,
        legend1=dict(orientation="h", yanchor="bottom", y=-0.3, xanchor="left", x=0),
        legend2=dict(orientation="h", yanchor="bottom", y=-0.3, xanchor="right", x=1),
        hovermode='x unified',
        hoverlabel=hover_label_style,
        **background_style
    )
    fig3.update_xaxes(title_text="Sync Speed [Blocks/sec]", row=1, col=1)
    fig3.update_yaxes(title_text="Density", row=1, col=1)
    fig3.update_xaxes(title_text="Time Between Samples [s]", row=1, col=2)
    fig3.update_yaxes(title_text="Density", row=1, col=2)

    # If only one dataset, hide the legend for the distribution plot
    if df_compare_display.empty:
        fig3.update_layout(showlegend=False)


    # --- Update Y-Axes for fig2 with specific colors for clarity ---
    fig2.update_yaxes(
        title_text="<b>Sync in Progress Time [s]</b>",
        secondary_y=False,
        color="#1f77b4",
        linecolor="#1f77b4",
        gridcolor='rgba(28, 119, 180, 0.3)',
        gridwidth=1,
        showgrid=True
    )
    fig2.update_yaxes(
        title_text="<b>Sync Speed [Blocks/sec]</b>",
        secondary_y=True,
        color=original_bps_color,
        linecolor=original_bps_color,
        gridcolor=f'rgba({int(original_bps_color[1:3], 16)}, {int(original_bps_color[3:5], 16)}, {int(original_bps_color[5:7], 16)}, 0.3)',
        gridwidth=1,
        showgrid=True
    )

    # Update layout and axes
    fig.update_layout(
        title_text=f'Block Height and Sync Speed vs. Sync Time (MA Window: {window})',
        height=600,
        hovermode='x unified', hoverlabel=hover_label_style,
        xaxis_showspikes=True, xaxis_spikemode='across', xaxis_spikedash='dot',
        yaxis_showspikes=True, yaxis_spikedash='dot', yaxis2_showspikes=True, yaxis2_spikedash='dot',
        legend=dict(orientation="v",
                    yanchor="top",
                    y=-0.2,
                    xanchor="left",
                    x=0,
                    itemclick='toggle',
                    itemdoubleclick='toggleothers'),
        template=graph_template,
        margin=dict(l=80, r=80, t=80, b=80),
        yaxis2=dict(
            side='right',
            overlaying='y',
            anchor='x',
            # Add some padding to the right of the secondary y-axis labels
            # to prevent them from being too close to the edge.
            title=dict(standoff=15),
            automargin=True,
        ),
        **background_style
    )
    # --- Update Y-Axes with specific colors for clarity ---
    # Primary Y-axis [Block Height] - uses default color (blue/cyan)
    fig.update_yaxes(
        title_text="<b>Block Height</b>",
        secondary_y=False,
        color="#1f77b4", # Sets tick and title font color
        linecolor="#1f77b4", # Sets axis line color
        gridcolor='rgba(28, 119, 180, 0.3)', # Color for the grid lines with some transparency
        gridwidth=1,
        showgrid=True
    )
    # Secondary Y-axis (Sync Speed) - uses orange color to match its traces
    fig.update_yaxes(
        title_text="<b>Sync Speed [Blocks/sec]</b>",
        secondary_y=True,
        color=original_bps_color, # Sets tick and title font color
        linecolor=original_bps_color, # Sets axis line color
        gridcolor=f'rgba({int(original_bps_color[1:3], 16)}, {int(original_bps_color[3:5], 16)}, {int(original_bps_color[5:7], 16)}, 0.3)', # Color for the grid lines with some transparency
        gridwidth=1,
        showgrid=True # Ensure grid is visible for the secondary axis
    )

    # --- Custom X-axis tick labels ---
    # Determine the range of the x-axis from all plotted data
    x_min, x_max = float('inf'), float('-inf')
    if not df_original_display.empty:
        x_min = min(x_min, df_original_display['Accumulated_sync_in_progress_time[s]'].min())
        x_max = max(x_max, df_original_display['Accumulated_sync_in_progress_time[s]'].max())
    if not df_compare_display.empty:
        x_min = min(x_min, df_compare_display['Accumulated_sync_in_progress_time[s]'].min())
        x_max = max(x_max, df_compare_display['Accumulated_sync_in_progress_time[s]'].max())

    if x_min != float('inf') and x_max != float('-inf'):
        # Generate about 5-10 tick values
        tick_values = pd.to_numeric(pd.Series(pd.date_range(start=pd.to_datetime(x_min, unit='s'), end=pd.to_datetime(x_max, unit='s'), periods=8)).astype(int) / 10**9)
        tick_text = [f"{int(val):,} sec<br>({format_seconds(val)})" for val in tick_values]
        fig.update_xaxes(tickvals=tick_values, ticktext=tick_text)

    fig.update_xaxes(title_text="Sync in Progress Time [s]", automargin=True)

    # --- Prepare outputs for dropdowns ---
    # The order of outputs is determined by Dash. We can get it from ctx.outputs_list.
    output_start_opts = [data_map[item['id']['prefix']]['options'] for item in ctx.outputs_grouping[4]]
    output_start_vals = [data_map[item['id']['prefix']]['start_block'] for item in ctx.outputs_grouping[5]]
    output_end_opts = [data_map[item['id']['prefix']]['options'] for item in ctx.outputs_grouping[6]]
    output_end_vals = [data_map[item['id']['prefix']]['end_block'] for item in ctx.outputs_grouping[7]]

    # --- Update total time display and metrics tables ---
    table_title_original = f"Original: {original_filename}"
    table_title_compare = f"Comparison: {compare_filename}" if compare_filename else "Comparison"
    summary_table = create_combined_summary_table(
        df_original_display,
        df_compare_display,
        table_title_original,
        table_title_compare
    )

    # --- Generate Raw Data Table ---
    table_children = []
    table_style = {'display': 'none'}
    if show_data_table:
        table_style = {'display': 'block'}
        data_col_names = {
            'Accumulated_sync_in_progress_time[s]': 'Sync Time [s]',
            'SyncTime_Formatted': 'Sync Time [Formatted]',
            'Blocks_per_Second': 'Sync Speed [Blocks/sec]'
        }
        cols_to_show = ['Block_height'] + list(data_col_names.keys())

        # Check if dataframes have been processed and have the necessary columns
        original_valid = not df_original_display.empty and all(c in df_original_display.columns for c in cols_to_show)
        compare_valid = not df_compare_display.empty and all(c in df_compare_display.columns for c in cols_to_show)
        
        if original_valid and compare_valid:
            # --- Merged Table Logic for Fixed Header ---
            df_orig_subset = df_original_display[cols_to_show].rename(columns=data_col_names)
            df_comp_subset = df_compare_display[cols_to_show].rename(columns=data_col_names)
            df_merged = pd.merge(
                df_orig_subset,
                df_comp_subset,
                on='Block_height',
                how='outer',
                suffixes=('_orig', '_comp')
            ).sort_values(by='Block_height').reset_index(drop=True)
            df_merged.rename(columns={'Block_height': 'Block Height'}, inplace=True)

            # --- Build Header Table ---
            left_border_style = {'borderLeft': '1px solid black'}
            # Define column widths for synchronization. This ensures header and body columns align.
            col_widths = ['16%'] + ['12%', '18%', '12%'] * 2
            col_group = html.Colgroup([html.Col(style={'width': w}) for w in col_widths])

            # --- Build Combined Header Table ---
            file_name_header_row = html.Tr([
                html.Th(""),  # Spacer for Block Height
                html.Th(f"Original: {original_filename}", colSpan=len(data_col_names), className="text-center", style={'fontWeight': 'normal', 'wordBreak': 'break-all'}),
                html.Th(f"Comparison: {compare_filename}", colSpan=len(data_col_names), className="text-center", style={'fontWeight': 'normal', 'wordBreak': 'break-all', **left_border_style})
            ])

            comparison_headers = [
                create_header_with_tooltip(col, col, style=left_border_style if i == 0 else None)
                for i, col in enumerate(data_col_names.values())
            ]
            column_name_header_row = html.Tr(
                [create_header_with_tooltip("Block Height", "Block Height")] +
                [create_header_with_tooltip(col, col) for col in data_col_names.values()] +
                comparison_headers
            )

            header_table_children = [html.Thead([file_name_header_row, column_name_header_row])]
            header_table_style = {
                'tableLayout': 'fixed', 'width': '100%',
                'position': 'sticky', 'top': 0, 'zIndex': 2, # type: ignore
                'backgroundColor': 'var(--bs-body-bg, white)'
            }
            header_table = dbc.Table([col_group] + header_table_children, bordered=True, className="mb-0", style=header_table_style)
            # --- Build Body Table ---
            body_rows = []
            for _, row in df_merged.iterrows():
                row_data = [html.Td(f"{int(row['Block Height']):,}" if pd.notna(row['Block Height']) else "")]
                
                numeric_metrics_info = {
                    'Sync Time [s]': {'higher_is_better': False},
                    'Sync Speed [Blocks/sec]': {'higher_is_better': True}
                }

                # Add original columns' cells
                for display_name in data_col_names.values():
                    orig_val = row.get(f"{display_name}_orig")

                    # Format the main value
                    if pd.isna(orig_val):
                        cell_content = [""]
                    elif isinstance(orig_val, float):
                        cell_content = [f"{orig_val:.2f}"]
                    else:
                        cell_content = [str(orig_val)]

                    # Calculate and add the difference if it's a numeric metric
                    if display_name in numeric_metrics_info:
                        comp_val = row.get(f"{display_name}_comp")
                        if pd.notna(orig_val) and pd.notna(comp_val):
                            diff = orig_val - comp_val
                            is_better = (diff > 0) if numeric_metrics_info[display_name]['higher_is_better'] else (diff < 0)
                            color_class = "text-success" if is_better else "text-danger" if diff != 0 else ""
                            if color_class:
                                if display_name == 'Sync Time [s]':
                                    diff_str = f" ({diff:+.2f}s)"
                                else:
                                    diff_str = f" ({diff:+.2f})"
                                cell_content.append(html.Span(diff_str, className=f"small {color_class} fw-bold"))
                    elif display_name == 'Sync Time [Formatted]':
                        orig_s_val = row.get('Sync Time [s]_orig')
                        comp_s_val = row.get('Sync Time [s]_comp')
                        if pd.notna(orig_s_val) and pd.notna(comp_s_val):
                            diff = orig_s_val - comp_s_val
                            color_class = "text-success" if diff < 0 else "text-danger" if diff != 0 else ""
                            if color_class:
                                sign = "+" if diff > 0 else "-"
                                diff_str = f" ({sign}{format_seconds(abs(diff))})"
                                cell_content.append(html.Span(diff_str, className=f"small {color_class} fw-bold"))
                    row_data.append(html.Td(cell_content))

                # Add comparison columns' cells
                for i, display_name in enumerate(data_col_names.values()):
                    cell_style = {'borderLeft': '1px solid black'} if i == 0 else {}
                    
                    comp_val = row.get(f"{display_name}_comp")
                    
                    # Format the main value
                    if pd.isna(comp_val):
                        cell_content = [""]
                    elif isinstance(comp_val, float):
                        cell_content = [f"{comp_val:.2f}"]
                    else:
                        cell_content = [str(comp_val)]

                    # Calculate and add the difference if it's a numeric metric
                    if display_name in numeric_metrics_info:
                        orig_val = row.get(f"{display_name}_orig")
                        if pd.notna(orig_val) and pd.notna(comp_val):
                            diff = comp_val - orig_val
                            is_better = (diff > 0) if numeric_metrics_info[display_name]['higher_is_better'] else (diff < 0)
                            color_class = "text-success" if is_better else "text-danger" if diff != 0 else ""
                            if color_class:
                                if display_name == 'Sync Time [s]':
                                    diff_str = f" ({diff:+.2f}s)"
                                else:
                                    diff_str = f" ({diff:+.2f})"
                                cell_content.append(html.Span(diff_str, className=f"small {color_class} fw-bold"))
                    elif display_name == 'Sync Time [Formatted]':
                        orig_s_val = row.get('Sync Time [s]_orig')
                        comp_s_val = row.get('Sync Time [s]_comp')
                        if pd.notna(orig_s_val) and pd.notna(comp_s_val):
                            diff = comp_s_val - orig_s_val
                            color_class = "text-success" if diff < 0 else "text-danger" if diff != 0 else ""
                            if color_class:
                                sign = "+" if diff > 0 else "-"
                                diff_str = f" ({sign}{format_seconds(abs(diff))})"
                                cell_content.append(html.Span(diff_str, className=f"small {color_class} fw-bold"))

                    row_data.append(html.Td(cell_content, style=cell_style))

                body_rows.append(html.Tr(row_data))

            body_table = dbc.Table([col_group, html.Tbody(body_rows)], striped=True, bordered=True, hover=True, style={'tableLayout': 'fixed', 'width': '100%', 'marginTop': '-1px'})
            
            # --- Combine into a single container ---
            scrollable_div = html.Div(
                [header_table, body_table],
                style={'maxHeight': '500px', 'overflowY': 'auto', 'width': '100%'}
            )
            table_children.append(scrollable_div)

        elif original_valid:
            # Single table
            col_names = {'Block_height': 'Block Height', **data_col_names}
            df_orig_table = df_original_display[cols_to_show].rename(columns=col_names)

            # --- Title (Non-scrollable) ---
            table_children.append(html.H6(f"Original: {original_filename}", style={'wordBreak': 'break-all'}))

            # --- Define column widths and create Colgroup ---
            col_widths = ['25%', '20%', '35%', '20%']
            col_group = html.Colgroup([html.Col(style={'width': w}) for w in col_widths])

            # --- Header Table ---
            header_table_children = [html.Thead(html.Tr([create_header_with_tooltip(col, col) for col in df_orig_table.columns]))]
            header_table_style = {
                'tableLayout': 'fixed', 'width': '100%',
                'position': 'sticky', 'top': 0, 'zIndex': 2, # type: ignore
                'backgroundColor': 'var(--bs-body-bg, white)'
            }
            header_table = dbc.Table([col_group] + header_table_children, bordered=True, className="mb-0", style=header_table_style)

            # --- Body Table ---
            body_rows = []
            for _, row in df_orig_table.iterrows():
                row_data = []
                for col in df_orig_table.columns:
                    val = row.get(col)
                    if pd.isna(val):
                        row_data.append(html.Td(""))
                    elif col == 'Block Height' and pd.notna(val):
                        row_data.append(html.Td(f"{int(val):,}"))
                    elif isinstance(val, float):
                        row_data.append(html.Td(f"{val:.2f}"))
                    else:
                        row_data.append(html.Td(val))
                body_rows.append(html.Tr(row_data))
            
            body_table = dbc.Table([col_group, html.Tbody(body_rows)], striped=True, bordered=True, hover=True, style={'tableLayout': 'fixed', 'width': '100%', 'marginTop': '-1px'})
            
            # --- Combine into a single container ---
            scrollable_div = html.Div(
                [header_table, body_table],
                style={'maxHeight': '500px', 'overflowY': 'auto', 'width': '100%'}
            )
            table_children.append(scrollable_div)
        elif compare_valid:
            # Single table for comparison data
            col_names = {'Block_height': 'Block Height', **data_col_names}
            df_comp_table = df_compare_display[cols_to_show].rename(columns=col_names)

            table_children.append(html.H6(f"Comparison: {compare_filename}", style={'wordBreak': 'break-all'}))
            col_widths = ['25%', '20%', '35%', '20%']
            col_group = html.Colgroup([html.Col(style={'width': w}) for w in col_widths])
            header_table_children = [html.Thead(html.Tr([create_header_with_tooltip(col, col) for col in df_comp_table.columns]))]
            header_table_style = {
                'tableLayout': 'fixed', 'width': '100%',
                'position': 'sticky', 'top': 0, 'zIndex': 2,
                'backgroundColor': 'var(--bs-body-bg, white)'
            }
            header_table = dbc.Table([col_group] + header_table_children, bordered=True, className="mb-0", style=header_table_style)
            body_rows = []
            for _, row in df_comp_table.iterrows():
                row_data = []
                for col in df_comp_table.columns:
                    val = row.get(col)
                    if pd.isna(val):
                        row_data.append(html.Td(""))
                    elif col == 'Block Height' and pd.notna(val):
                        row_data.append(html.Td(f"{int(val):,}"))
                    elif isinstance(val, float):
                        row_data.append(html.Td(f"{val:.2f}"))
                    else:
                        row_data.append(html.Td(val))
                body_rows.append(html.Tr(row_data))
            body_table = dbc.Table([col_group, html.Tbody(body_rows)], striped=True, bordered=True, hover=True, style={'tableLayout': 'fixed', 'width': '100%', 'marginTop': '-1px'}) # type: ignore
            scrollable_div = html.Div([header_table, body_table], style={'maxHeight': '500px', 'overflowY': 'auto', 'width': '100%'})
            table_children.append(scrollable_div)
        else:
            table_children = [html.P("No data to display in table.")]

    return fig, fig2, fig3, summary_table, output_start_opts, output_start_vals, output_end_opts, output_end_vals, {}, table_children, table_style

@app.callback(
    Output('action-feedback-store', 'data', allow_duplicate=True),
    Input({'type': 'reset-view-button', 'prefix': dash.dependencies.ALL}, 'n_clicks'),
    prevent_initial_call=True
)
def handle_reset_feedback(n_clicks_list):
    if not any(n_clicks_list):
        raise dash.exceptions.PreventUpdate
    return {
        'title': 'View Reset',
        'body': 'The block range filter has been reset to show all available data.'
    }

@app.callback(
    Output('no-file-alert', 'is_open'),
    Input('original-data-store', 'data'),
    prevent_initial_call=True
)
def hide_no_file_alert(original_data):
    if original_data:
        return False
    return True


# --- Callback to show tooltip modal ---
@app.callback(
    [Output("tooltip-modal", "is_open"),
     Output("tooltip-modal-title", "children"),
     Output("tooltip-modal-body", "children")],
    [Input({'type': 'info-icon', 'metric': dash.dependencies.ALL}, 'n_clicks')],
    prevent_initial_call=True
)
def show_tooltip_modal(n_clicks):
    ctx = dash.callback_context
    # This is the most robust way to check for a real user click.
    # It verifies that the callback was triggered and that the trigger value is a positive integer (not None or 0).
    # This prevents the modal from appearing when its inputs are re-rendered by another callback (e.g., by the slider).
    if not ctx.triggered or not ctx.triggered[0]['value']:
        raise dash.exceptions.PreventUpdate
    triggered_id_dict = ctx.triggered_id
    # A final safety check in case the triggered_id is None
    if not triggered_id_dict:
        raise dash.exceptions.PreventUpdate
    metric_id = triggered_id_dict['metric']
    # Handle prefixed metric IDs from metadata cards (e.g., "Original-Hostname")
    # and non-prefixed IDs from other parts of the app.
    if '-' in metric_id and any(metric_id.startswith(p) for p in ['Original-', 'Comparison-']):
        metric_name = metric_id.split('-', 1)[1]
    else:
        metric_name = metric_id
    info = tooltip_texts.get(metric_name, {})
    title = info.get('title', 'Information')
    body_text = info.get('body', 'No details available for this metric.')
    # Split body into paragraphs for better formatting
    body_components = [html.P(p) for p in body_text.split('\n\n')]

    return True, title, body_components

@app.callback(
    [Output("action-feedback-modal", "is_open"),
     Output("action-feedback-modal-title", "children"),
     Output("action-feedback-modal-body", "children"),
     Output("open-report-button", "style")],
    [Input("action-feedback-store", "data"),
     Input("action-feedback-modal-close", "n_clicks")],
    [State("action-feedback-modal", "is_open")],
    prevent_initial_call=True,
)
def show_action_feedback_modal(feedback_data, n_clicks, is_open):
    ctx = dash.callback_context
    triggered_id = ctx.triggered_id

    # If the close button was clicked, close the modal
    if triggered_id == "action-feedback-modal-close":
        return False, dash.no_update, dash.no_update, {'display': 'none'}

    # If new feedback data arrived, open the modal and set content
    if triggered_id == "action-feedback-store" and feedback_data:
        title = feedback_data.get('title', 'Notification')
        body = feedback_data.get('body', 'An action was completed.')

        # Style to ensure long paths wrap correctly inside the modal
        p_style = {'overflowWrap': 'break-word', 'wordWrap': 'break-word'}

        if isinstance(body, list):
            body_components = [html.P(p, style=p_style) for p in body]
        else:
            body_components = [html.P(body, style=p_style)]
        
        button_style = {'display': 'inline-block'} if title == 'Reports Saved' else {'display': 'none'}
        return True, title, body_components, button_style

    # In all other cases, don't change the modal state
    return is_open, dash.no_update, dash.no_update, dash.no_update

@app.callback(
    Output({'type': 'unsaved-changes-badge', 'prefix': dash.dependencies.ALL}, 'style', allow_duplicate=True),
    Input('unsaved-changes-store', 'data'),
    [State({'type': 'unsaved-changes-badge', 'prefix': dash.dependencies.ALL}, 'id')],
    prevent_initial_call=True
)
def update_unsaved_changes_badge(unsaved_data, ids):
    if not unsaved_data:
        return [dash.no_update] * len(ids)
    
    styles = []
    for component_id in ids:
        prefix = component_id['prefix']
        if unsaved_data.get(prefix, False):
            styles.append({'display': 'inline-block', 'verticalAlign': 'middle'})
        else:
            styles.append({'display': 'none', 'verticalAlign': 'middle'})
    return styles

@app.callback(
    [Output('original-data-store', 'data', allow_duplicate=True),
     Output('unsaved-changes-store', 'data', allow_duplicate=True)],
    Input({'type': 'add-metadata-button', 'prefix': 'Original'}, 'n_clicks'),
    [
        State({'type': 'metadata-key-input', 'prefix': 'Original'}, 'value'),
        State({'type': 'metadata-value-input', 'prefix': 'Original'}, 'value'),
        State('original-data-store', 'data'),
        State('unsaved-changes-store', 'data')
    ],
    prevent_initial_call=True
)
def add_original_metadata(n_clicks, key, value, store_data, unsaved_data):
    if not n_clicks or not key or value is None:
        raise dash.exceptions.PreventUpdate

    if store_data and 'metadata' in store_data:
        store_data['metadata'][key.strip()] = value.strip()
        unsaved_data['Original'] = True
        return store_data, unsaved_data
    
    raise dash.exceptions.PreventUpdate

@app.callback(
    [Output('original-data-store', 'data', allow_duplicate=True),
     Output('unsaved-changes-store', 'data', allow_duplicate=True)],
    Input({'type': 'metadata-input', 'prefix': 'Original', 'key': dash.dependencies.ALL}, 'value'),
    [State('original-data-store', 'data'),
     State('unsaved-changes-store', 'data')],
    prevent_initial_call=True
)
def update_original_metadata(values, store_data, unsaved_data):
    ctx = dash.callback_context
    if not ctx.triggered_id:
        raise dash.exceptions.PreventUpdate
    
    key = ctx.triggered_id['key']
    value = ctx.triggered[0]['value']

    if store_data and 'metadata' in store_data and store_data['metadata'].get(key) != value:
        store_data['metadata'][key] = value
        unsaved_data['Original'] = True
        return store_data, unsaved_data
    
    raise dash.exceptions.PreventUpdate

@app.callback(
    [Output('compare-data-store', 'data', allow_duplicate=True),
     Output('unsaved-changes-store', 'data', allow_duplicate=True)],
    Input({'type': 'metadata-input', 'prefix': 'Comparison', 'key': dash.dependencies.ALL}, 'value'),
    [State('compare-data-store', 'data'),
     State('unsaved-changes-store', 'data')],
    prevent_initial_call=True
)
def update_compare_metadata(values, store_data, unsaved_data):
    ctx = dash.callback_context
    if not ctx.triggered_id:
        raise dash.exceptions.PreventUpdate
    
    key = ctx.triggered_id['key']
    value = ctx.triggered[0]['value']

    if store_data and 'metadata' in store_data and store_data['metadata'].get(key) != value:
        store_data['metadata'][key] = value
        unsaved_data['Comparison'] = True
        return store_data, unsaved_data
    
    raise dash.exceptions.PreventUpdate

@app.callback(
    [Output('compare-data-store', 'data', allow_duplicate=True),
     Output('unsaved-changes-store', 'data', allow_duplicate=True)],
    Input({'type': 'add-metadata-button', 'prefix': 'Comparison'}, 'n_clicks'),
    [
        State({'type': 'metadata-key-input', 'prefix': 'Comparison'}, 'value'),
        State({'type': 'metadata-value-input', 'prefix': 'Comparison'}, 'value'),
        State('compare-data-store', 'data'),
        State('unsaved-changes-store', 'data')
    ],
    prevent_initial_call=True
)
def add_compare_metadata(n_clicks, key, value, store_data, unsaved_data):
    if not n_clicks or not key or value is None:
        raise dash.exceptions.PreventUpdate

    if store_data and 'metadata' in store_data:
        store_data['metadata'][key.strip()] = value.strip()
        unsaved_data['Comparison'] = True
        return store_data, unsaved_data
    
    raise dash.exceptions.PreventUpdate

def write_csv_new(store_data, filter_range, start_block, end_block):
    if not store_data:
        return "No data in store to save."

    filename = store_data.get('filename', 'unknown_file.csv')
    metadata = store_data.get('metadata', {})
    df_json = store_data.get('data')

    if not df_json:
        return f"No data content found for '{filename}'."

    df = pd.read_json(io.StringIO(df_json), orient='split')

    # Apply filters based on options
    suffix = ""
    if filter_range and start_block is not None and end_block is not None:
        df = df[(df['Block_height'] >= start_block) & (df['Block_height'] <= end_block)]
        suffix += f"_range_{int(start_block)}-{int(end_block)}"

    # Create the new file content as a string
    output = io.StringIO()
    
    # Write metadata
    if metadata:
        output.write("Property;Value\n")
        for key, value in metadata.items():
            output.write(f"{key};{value}\n")
        output.write(";;\n") # Separator

    # Write dataframe
    df.to_csv(output, sep=';', index=False)
    
    # Save to a new file
    try:
        saved_dir = os.path.join(SCRIPT_DIR, "measurements")
        os.makedirs(saved_dir, exist_ok=True)
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        base_filename = os.path.basename(filename)
        base, ext = os.path.splitext(base_filename)

        # Add hostname to the filename if available
        hostname_part = ""
        hostname = metadata.get('Hostname')
        if hostname:
            # Sanitize hostname for use in a filename
            sanitized_hostname = re.sub(r'[^\w\.\-]', '_', hostname)
            hostname_part = f"_hostname_{sanitized_hostname}"

        # Sequentially remove known suffixes from the end to get the clean base name
        base = re.sub(r'_\d+$', '', base)  # sequence number (_1)
        base = re.sub(r'_\d{8}_\d{6}$', '', base)  # timestamp
        base = re.sub(r'_hostname_[\w\.\-]+$', '', base) # hostname
        base = re.sub(r'_range_\d+-\d+$', '', base) # range suffix

        # Construct the new filename
        new_base_filename = f"{base}{suffix}{hostname_part}_{timestamp}"
        
        # Find a unique filename by appending a counter if necessary
        filepath = os.path.join(saved_dir, f"{new_base_filename}.csv")
        counter = 1
        while os.path.exists(filepath):
            filepath = os.path.join(saved_dir, f"{new_base_filename}_{counter}.csv")
            counter += 1
        
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(output.getvalue())
        
        return f"File with updated data saved to: {filepath}"
    except Exception as e:
        return f"Error saving file: {e}"

def write_csv_overwrite(store_data, filter_range, start_block, end_block):
    if not store_data:
        return "No data in store to save."

    filename = store_data.get('filename', 'unknown_file.csv')
    metadata = store_data.get('metadata', {})
    df_json = store_data.get('data')

    if not df_json:
        return f"No data content found for '{filename}'."

    df = pd.read_json(io.StringIO(df_json), orient='split')

    timestamp_part = ""
    suffix = ""
    if filter_range and start_block is not None and end_block is not None:
        df = df[(df['Block_height'] >= start_block) & (df['Block_height'] <= end_block)]
        suffix += f"_range_{int(start_block)}-{int(end_block)}"
        timestamp_part = f"_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"

    output = io.StringIO()
    if metadata:
        output.write("Property;Value\n")
        for key, value in metadata.items():
            output.write(f"{key};{value}\n")
        output.write(";;\n")

    df.to_csv(output, sep=';', index=False)

    try:
        saved_dir = os.path.join(SCRIPT_DIR, "measurements")
        os.makedirs(saved_dir, exist_ok=True)
        base_filename = os.path.basename(filename)
        base, ext = os.path.splitext(base_filename)

        # Clean up old suffixes
        base = re.sub(r'_\d+$', '', base)
        base = re.sub(r'_\d{8}_\d{6}$', '', base)
        base = re.sub(r'_hostname_[\w\.\-]+$', '', base)
        base = re.sub(r'_range_\d+-\d+$', '', base)

        hostname_part = ""
        hostname = metadata.get('Hostname')
        if hostname:
            sanitized_hostname = re.sub(r'[^\w\.\-]', '_', hostname)
            hostname_part = f"_hostname_{sanitized_hostname}"

        new_filename = f"{base}{suffix}{hostname_part}{timestamp_part}.csv"
        filepath = os.path.join(saved_dir, new_filename)

        with open(filepath, "w", encoding="utf-8") as f:
            f.write(output.getvalue())
        
        return f"File saved to: {filepath}"
    except Exception as e:
        return f"Error saving file: {e}"

@app.callback(
    [Output('action-feedback-store', 'data', allow_duplicate=True),
     Output('unsaved-changes-store', 'data', allow_duplicate=True),
     Output('save-callback-output', 'figure', allow_duplicate=True),
     Output('reset-upload-store', 'data', allow_duplicate=True)],
    [Input({'type': 'save-as-button', 'prefix': dash.dependencies.ALL}, 'n_clicks'),
     Input({'type': 'save-overwrite-button', 'prefix': dash.dependencies.ALL}, 'n_clicks')],
    [State('original-data-store', 'data'),
     State('compare-data-store', 'data'),
     State({'type': 'save-filter-range-check', 'prefix': dash.dependencies.ALL}, 'value'),
     State({'type': 'start-block-dropdown', 'prefix': dash.dependencies.ALL}, 'value'),
     State({'type': 'end-block-dropdown', 'prefix': dash.dependencies.ALL}, 'value'),
     State('unsaved-changes-store', 'data')],
    prevent_initial_call=True
)
def save_csv(n_clicks_as, n_clicks_overwrite, original_data, compare_data, filter_range_values, start_block_vals, end_block_vals, unsaved_data): # type: ignore
    triggered_id = dash.callback_context.triggered_id
    if not triggered_id or not (any(n_clicks_as) or any(n_clicks_overwrite)):
        raise dash.exceptions.PreventUpdate

    prefix = triggered_id['prefix']
    save_as = triggered_id['type'] == 'save-as-button'

    filter_range = False
    for i, comp_id in enumerate(dash.callback_context.states_list[2]): # type: ignore
        if comp_id['id']['prefix'] == prefix:
            filter_range = bool(filter_range_values[i])
            break

    start_block = None
    end_block = None

    # Find the correct start and end block values from the lists based on the triggered prefix
    start_block_states = dash.callback_context.states_list[3]
    for i, state in enumerate(start_block_states):
        if state['id']['prefix'] == prefix:
            start_block = start_block_vals[i]
            break

    end_block_states = dash.callback_context.states_list[4]
    for i, state in enumerate(end_block_states):
        if state['id']['prefix'] == prefix:
            end_block = end_block_vals[i]
            break

    message = "An unknown error occurred."
    data_to_save = original_data if prefix == 'Original' else compare_data

    upload_id_to_reset = None

    if data_to_save:
        # Create a copy to avoid modifying the state directly
        new_unsaved_data = unsaved_data.copy()
        if save_as:
            message = write_csv_new(data_to_save, filter_range, start_block, end_block)
        else:
            message = write_csv_overwrite(data_to_save, filter_range, start_block, end_block)
        
        if 'Error' not in message:
            new_unsaved_data[prefix] = False
            upload_id_to_reset = 'upload-original-progress' if prefix == 'Original' else 'upload-compare-progress'

    return {'title': 'Save to CSV', 'body': message}, new_unsaved_data, {}, upload_id_to_reset

@app.callback(
    Output('theme-switch', 'value'),
    Output('theme-stylesheet', 'href', allow_duplicate=True),
    Input('theme-store', 'data'),
    prevent_initial_call='initial_duplicate'
)
def load_initial_theme(stored_theme):
    # Default to dark theme if nothing is stored
    if stored_theme == 'light':
        return False, dbc.themes.BOOTSTRAP
    return True, dbc.themes.DARKLY

@app.callback(
    Output('theme-store', 'data', allow_duplicate=True),
    Output('theme-stylesheet', 'href', allow_duplicate=True),
    Input('theme-switch', 'value'),
    prevent_initial_call=True
)
def switch_theme(is_dark):
    if is_dark:
        return 'dark', dbc.themes.DARKLY
    return 'light', dbc.themes.BOOTSTRAP

@app.callback( # type: ignore
    [Output('original-metadata-display', 'children'),
     Output('original-metadata-display', 'style'),
     Output('compare-metadata-display', 'children'),
     Output('compare-metadata-display', 'style')],
    [Input('original-data-store', 'data'),
     Input('compare-data-store', 'data')])
def update_metadata_display(original_data, compare_data):
    def create_system_info_card(data, title_prefix):
        if not data:
            return None
        metadata = data.get('metadata', {})
        filename = data.get('filename', 'data file')

        card_header = dbc.CardHeader(
            dbc.Row([
                dbc.Col(f"{title_prefix} System Info: {os.path.basename(filename)}", className="fw-bold"),
                dbc.Col([
                    dbc.Badge("Unsaved", color="warning", className="me-2", id={'type': 'unsaved-changes-badge', 'prefix': title_prefix}, style={'display': 'none', 'verticalAlign': 'middle'}),
                ], width="auto", className="d-flex align-items-center")
            ], align="center", justify="between")
        )

        # Define preferred order for display to ensure consistency
        preferred_order = [
            'Signum Version', 'Hostname', 'OS Name', 'OS Version', 'OS Architecture',
            'Java Version', 'Available Processors', 'Max Memory (MB)', 'Total RAM (MB)',
            'Database Type', 'Database Version'
        ]

        list_group_items = []

        def create_list_item(key, value):
            key_with_icon = [html.B(f"{key}:")]
            if key in tooltip_texts:
                # Make the ID unique by prefixing it with the card type (Original/Comparison)
                unique_metric_id = f"{title_prefix}-{key}"
                info_icon = html.Span([ # type: ignore
                    "\u00A0",  # Non-breaking space
                    html.I(className="bi bi-info-circle-fill text-info align-middle", style={'fontSize': '1.1em'}),
                ], id={'type': 'info-icon', 'metric': unique_metric_id}, style={'cursor': 'pointer', 'marginLeft': '5px'}, title='Click for more info', n_clicks=0) # type: ignore
                key_with_icon.append(info_icon)

            delete_button = html.Span([
                html.I(className="bi bi-dash-circle-fill text-danger align-middle", style={'fontSize': '1.1em'}),
            ],
                id={'type': 'delete-metadata-button', 'prefix': title_prefix, 'key': key},
                n_clicks=0,
                style={'cursor': 'pointer'},
                title='Delete item',
                className="ms-2"
            )

            return dbc.ListGroupItem(
                [
                    html.Div(key_with_icon, style={'display': 'flex', 'alignItems': 'center'}),
                    html.Div([
                        dbc.Input(
                            id={'type': 'metadata-input', 'prefix': title_prefix, 'key': key},
                            value=str(value), type='text', className="text-end text-muted", size="sm",
                            style={'border': 'none', 'backgroundColor': 'transparent', 'boxShadow': 'none', 'padding': '0', 'margin': '0', 'height': 'auto'},
                            debounce=True),
                        delete_button
                    ], className="d-flex align-items-center")
                ],
                className="d-flex justify-content-between align-items-center p-2"
            )

        # Add preferred keys first, in the specified order
        for key in preferred_order:
            if key in metadata:
                list_group_items.append(create_list_item(key, metadata[key]))

        # Add any other keys that were not in the preferred list
        # This makes the function robust to future additions
        for key, value in metadata.items():
            if key not in preferred_order:
                list_group_items.append(create_list_item(key, metadata[key]))

        card_body = dbc.ListGroup(list_group_items, flush=True)
        return dbc.Card([card_header, card_body])

    def create_controls_card(data, title_prefix):
        list_group_items = []
        # Add block selectors
        start_block_selector = dbc.ListGroupItem(
            [
                html.Div([
                    html.B("Start Block Height:"),
                    html.Span([ # type: ignore
                        "\u00A0", html.I(className="bi bi-info-circle-fill text-info align-middle", style={'fontSize': '1.1em'}),
                    ], id={'type': 'info-icon', 'metric': f'{title_prefix}-Start Block Height'}, style={'cursor': 'pointer', 'marginLeft': '5px'}, title='Click for more info', n_clicks=0)
                ], style={'display': 'flex', 'alignItems': 'center'}),
                html.Div(dcc.Dropdown(id={'type': 'start-block-dropdown', 'prefix': title_prefix}, clearable=False, placeholder="Select start block"), style={'width': '50%'})
            ],
            className="d-flex justify-content-between align-items-center p-2"
        )
        list_group_items.append(start_block_selector)

        end_block_selector = dbc.ListGroupItem(
            [
                html.Div([
                    html.B("End Block Height:"),
                    html.Span([ # type: ignore
                        "\u00A0", html.I(className="bi bi-info-circle-fill text-info align-middle", style={'fontSize': '1.1em'}),
                    ], id={'type': 'info-icon', 'metric': f'{title_prefix}-End Block Height'}, style={'cursor': 'pointer', 'marginLeft': '5px'}, title='Click for more info', n_clicks=0)
                ], style={'display': 'flex', 'alignItems': 'center'}),
                html.Div(dcc.Dropdown(id={'type': 'end-block-dropdown', 'prefix': title_prefix}, clearable=False, placeholder="Select end block"), style={'width': '50%'})
            ],
            className="d-flex justify-content-between align-items-center p-2"
        )
        list_group_items.append(end_block_selector)
        # --- New UI elements for adding metadata ---
        add_metadata_form = dbc.ListGroupItem([
            dbc.Row([
                dbc.Col(dbc.Input(id={'type': 'metadata-key-input', 'prefix': title_prefix}, placeholder='New Property', type='text', size='sm'), width=4),
                dbc.Col(dbc.Input(id={'type': 'metadata-value-input', 'prefix': title_prefix}, placeholder='Value', type='text', size='sm'), width=4),
                dbc.Col(
                    html.Div([
                        dbc.Button("Add", id={'type': 'add-metadata-button', 'prefix': title_prefix}, color="primary", size="sm", className="w-100"),
                        html.Span([ # type: ignore
                            "\u00A0", html.I(className="bi bi-info-circle-fill text-info align-middle", style={'fontSize': '1.1em'}),
                        ], id={'type': 'info-icon', 'metric': f'{title_prefix}-Add Metadata'}, style={'cursor': 'pointer', 'marginLeft': '5px'}, title='Click for more info', n_clicks=0)
                    ], className="d-flex align-items-center"), width=4)
            ], align="center", className="g-2") # g-2 for gutter
        ], className="p-2")

        list_group_items.append(add_metadata_form)
        controls_bar = dbc.ListGroupItem([
            dbc.Row([
                # Left side: View and Clear controls
                dbc.Col([
                    dbc.Button("Reset View", id={'type': 'reset-view-button', 'prefix': title_prefix}, color="secondary", size="sm", className="me-2"),
                    html.Span([ # type: ignore
                        "\u00A0",
                        html.I(className="bi bi-info-circle-fill text-info align-middle", style={'fontSize': '1.1em'}),
                    ], id={'type': 'info-icon', 'metric': f'{title_prefix}-Reset View'}, style={'cursor': 'pointer'}, title='Click for more info', n_clicks=0), # type: ignore
                    html.Div("|", className="text-muted mx-2"),
                    dbc.Button("Clear CSV", id={'type': 'clear-csv-button', 'prefix': title_prefix}, color="warning", size="sm", className="me-2"),
                    html.Span([ # type: ignore
                        "\u00A0",
                        html.I(className="bi bi-info-circle-fill text-info align-middle", style={'fontSize': '1.1em'}),
                    ], id={'type': 'info-icon', 'metric': f'{title_prefix}-Clear CSV'}, style={'cursor': 'pointer'}, title='Click for more info', n_clicks=0), # type: ignore
                ], width="auto", className="d-flex align-items-center"),
                # Right side: Save controls
                dbc.Col([
                    dcc.Checklist(
                        options=[{'label': ' Filter range', 'value': 'filter'}],
                        value=[],
                        id={'type': 'save-filter-range-check', 'prefix': title_prefix},
                        inline=True,
                        className="me-1 custom-checklist"
                    ),
                    html.Span([ # type: ignore
                        "\u00A0",
                        html.I(className="bi bi-info-circle-fill text-info align-middle", style={'fontSize': '1.1em'}),
                    ], id={'type': 'info-icon', 'metric': f'{title_prefix}-Save Filtered Range'}, style={'cursor': 'pointer'}, title='Click for more info', n_clicks=0), # type: ignore
                    html.Div("|", className="text-muted mx-2"),
                    dbc.Button("Save", id={'type': 'save-overwrite-button', 'prefix': title_prefix}, size="sm", color="primary", className="me-1"),
                    html.Span([ # type: ignore
                        "\u00A0",
                        html.I(className="bi bi-info-circle-fill text-info align-middle", style={'fontSize': '1.1em'}),
                    ], id={'type': 'info-icon', 'metric': f'{title_prefix}-Save'}, style={'cursor': 'pointer'}, title='Click for more info', n_clicks=0), # type: ignore
                    html.Div("|", className="text-muted mx-2"),
                    dbc.Button("Save As...", id={'type': 'save-as-button', 'prefix': title_prefix}, size="sm", color="success", className="me-1"), # type: ignore
                    html.Span([
                        "\u00A0",  # Non-breaking space
                        html.I(className="bi bi-info-circle-fill text-info align-middle", style={'fontSize': '1.1em'}),
                    ], id={'type': 'info-icon', 'metric': f'{title_prefix}-Save As...'}, style={'cursor': 'pointer'}, title='Click for more info', n_clicks=0), # type: ignore
                ], width="auto", className="d-flex align-items-center justify-content-end")
            ], align="center", justify="between")
        ], className="p-2")
        list_group_items.append(controls_bar)

        card_body = dbc.ListGroup(list_group_items, flush=True)
        return dbc.Card(card_body, className="mt-3")

    original_display = html.Div([
        create_system_info_card(original_data, "Original"),
        create_controls_card(original_data, "Original")
    ])
    original_style = {'display': 'block'} if original_data else {'display': 'none'}

    compare_display = html.Div([
        create_system_info_card(compare_data, "Comparison"),
        create_controls_card(compare_data, "Comparison")
    ])
    compare_style = {'display': 'block'} if compare_data else {'display': 'none'}

    return original_display, original_style, compare_display, compare_style

@app.callback(
    [Output('original-data-store', 'data', allow_duplicate=True),
     Output('compare-data-store', 'data', allow_duplicate=True),
     Output('unsaved-changes-store', 'data', allow_duplicate=True)],
    [Input({'type': 'delete-metadata-button', 'prefix': dash.dependencies.ALL, 'key': dash.dependencies.ALL}, 'n_clicks')],
    [State('original-data-store', 'data'),
     State('compare-data-store', 'data'),
     State('unsaved-changes-store', 'data')],
    prevent_initial_call=True
)
def delete_metadata_item(n_clicks, original_data, compare_data, unsaved_data):
    ctx = dash.callback_context
    if not ctx.triggered or not any(n['value'] for n in ctx.triggered if n['value'] is not None):
        raise dash.exceptions.PreventUpdate

    triggered_id = ctx.triggered_id
    prefix = triggered_id['prefix']
    key_to_delete = triggered_id['key']

    if prefix == 'Original' and original_data and 'metadata' in original_data:
        if key_to_delete in original_data['metadata']:
            del original_data['metadata'][key_to_delete]
            unsaved_data['Original'] = True
            return original_data, dash.no_update, unsaved_data
    elif prefix == 'Comparison' and compare_data and 'metadata' in compare_data:
        if key_to_delete in compare_data['metadata']:
            del compare_data['metadata'][key_to_delete]
            unsaved_data['Comparison'] = True
            return dash.no_update, compare_data, unsaved_data

    raise dash.exceptions.PreventUpdate

app.clientside_callback(
    """
    function(upload_id) {
        if (!upload_id) {
            return window.dash_clientside.no_update;
        }
        // This is a bit of a hack to reset the dcc.Upload component.
        // It finds the 'x' (remove) button inside the upload component and clicks it.
        // This is more reliable than trying to set `contents` to null from the server.
        const uploadElement = document.getElementById(upload_id);
        if (uploadElement) {
            // The actual clickable element is often a div or span inside the main component
            const removeButton = uploadElement.querySelector('div > div > span');
            if (removeButton) {
                removeButton.click();
            }
        }
        return window.dash_clientside.no_update;
    }
    """,
    Output('reset-upload-store', 'data', allow_duplicate=True),
    Input('reset-upload-store', 'data'),
    prevent_initial_call=True
)
# --- Callback to apply custom dark theme styles to dropdowns ---

# --- Callbacks for saving HTML report ---
app.clientside_callback(
    """
    async function(n_clicks, slider_value_index, progress_figure, blockheight_figure, distribution_figure) {
        if (!n_clicks) {
            // This is the initial call or a callback update where the button wasn't clicked.
            return [window.dash_clientside.no_update, window.dash_clientside.no_update];
        }

        try {
            const rootElement = document.documentElement;
            if (!rootElement) {
                return ['CLIENTSIDE_ERROR: root element (html) not found.', null];
            }
            // Clone the container to avoid modifying the live DOM
            const clone = rootElement.cloneNode(true);

            const isDarkTheme = document.documentElement.getAttribute('data-bs-theme') === 'dark';

            // --- Replace interactive elements with static text ---

            // 1. Dropdowns
            clone.querySelectorAll('div[id*="start-block-dropdown"], div[id*="end-block-dropdown"]').forEach(clonedDropdown => {
                const originalDropdown = document.getElementById(clonedDropdown.id);
                const wrapper = clonedDropdown.parentElement;
                if (originalDropdown && wrapper && wrapper.parentElement) {
                    const originalDropdownValue = originalDropdown.querySelector('.Select-value-label, .Select__single-value');
                    const valueText = originalDropdownValue ? originalDropdownValue.textContent : 'N/A';
                    const staticEl = document.createElement('div');
                    staticEl.textContent = valueText;
                    staticEl.className = 'text-end';
                    staticEl.style.width = '50%';
                    staticEl.style.fontSize = '0.875em';
                    staticEl.style.color = 'var(--bs-body-color)';
                    wrapper.parentElement.replaceChild(staticEl, wrapper);
                }
            });

            // 2. Metadata Inputs
            clone.querySelectorAll('input[id*="metadata-input"]').forEach(input => {
                if (input.parentElement) {
                    const valueText = input.value || 'N/A';
                    const staticEl = document.createElement('div');
                    staticEl.textContent = valueText;
                    staticEl.className = 'text-end text-muted';
                    staticEl.style.fontSize = '0.875em';
                    input.parentElement.replaceChild(staticEl, input);
                }
            });

            // 3. Slider
            const ma_windows = [10, 100, 200, 300, 400, 500];
            const window_size = ma_windows[slider_value_index];
            const sliderContainer = clone.querySelector('#ma-slider-container');
            if (sliderContainer) {
                const staticEl = document.createElement('div');
                staticEl.textContent = `Moving Average Window: ${window_size} blocks`;
                staticEl.className = 'mt-3'; // Add some margin
                staticEl.style.fontWeight = 'bold';
                sliderContainer.parentNode.replaceChild(staticEl, sliderContainer);
            }

            // --- Remove all interactive/unnecessary elements ---
            const selectorsToRemove = [
                // General UI controls
                '#save-button', '#theme-switch', '.bi-sun-fill', '.bi-moon-stars-fill',
                '#show-data-table-switch-container', '#original-upload-container', '#compare-upload-container', 'span[id*="info-icon"]',
                'script', '#loading-overlay',
                '#_dash-dev-tools-ui-container', // Dash Dev Tools main container
                // Dash developer/debug panels and error overlays (Dash 3.x)
                '#_dash-debug-menu',
                '.dash-debug-menu',
                '.dash-debug-menu__button',
                '.dash-debug-menu__title',
                '.dash-debug-menu__version',
                '.dash-debug-menu__server',
                '.dash-debug-menu__status',
                '.dash-debug-menu__icon',
                '.dash-debug-menu__indicator',
                '.dash-debug-menu__status--success',
                '.dash-debug-menu__status--error',
                '.bi-arrow-bar-left',
                '.bi-arrow-left',
                '.bi-arrow-return-left',
                '.dash-error-message',
                '.dash-fe-error__title',
                '.dash-fe-error__message',
                '.dash-fe-error__stack',
                '.dash-dev-tools-menu',
                '.dash-dev-tools-menu__button',
                '.dash-dev-tools-menu__title',
                '.dash-dev-tools-menu__version',
                '.dash-dev-tools-menu__server',
                // Reload and discard buttons
                '#reload-original-button', '#reload-compare-button',
                '#discard-original-button', '#discard-compare-button',
                // Individual metadata controls
                'span[id*="delete-metadata-button"]',
                'span[id*="unsaved-changes-badge"]'
            ];
            // Remove any undefined or empty selectors to avoid JS errors
            const validSelectorsToRemove = selectorsToRemove.filter(s => typeof s === 'string' && s.length > 0);
            const extraArrowSelectors = [
                '.dash-debug-menu__outer'
                ,'.dash-debug-menu__outer--expanded'
                ,'.dash-debug-menu__toggle'
                ,'.dash-debug-menu__toggle--expanded'
            ];
            const allSelectorsToRemove = validSelectorsToRemove.concat(extraArrowSelectors);
            clone.querySelectorAll(allSelectorsToRemove.join(', ')).forEach(el => el.remove());

            // --- Remove entire rows for certain controls ---
            const rowSelectorsToRemove = [
                'button[id*="add-metadata-button"]', // The "Add" button and its row
                'button[id*="reset-view-button"]', // The entire bar with Reset, Clear, and Save buttons
                'button[id*="save-overwrite-button"]'
            ];
            clone.querySelectorAll(rowSelectorsToRemove.join(', ')).forEach(el => {
                const parentRow = el.closest('.list-group-item');
                if (parentRow) parentRow.remove();
            });

            // --- Convert all Plotly graphs to static images ---
            const graphsToConvert = [
                { id: 'progress-graph', figure: progress_figure },
                { id: 'blockheight-vs-speed-graph', figure: blockheight_figure },
                { id: 'distribution-graph', figure: distribution_figure }
            ];

            for (const graphInfo of graphsToConvert) {
                const graphDiv = clone.querySelector(`#${graphInfo.id}`);
                const originalGraphDiv = document.getElementById(graphInfo.id);

                if (graphDiv && originalGraphDiv && window.Plotly && graphInfo.figure) {
                    const tempDiv = document.createElement('div');
                    // Position it off-screen
                    tempDiv.style.position = 'absolute';
                    tempDiv.style.left = '-9999px';
                    tempDiv.style.width = originalGraphDiv.offsetWidth + 'px';
                    tempDiv.style.height = originalGraphDiv.offsetHeight + 'px';
                    document.body.appendChild(tempDiv);

                    try {
                        const data = JSON.parse(JSON.stringify(graphInfo.figure.data));
                        const layout = JSON.parse(JSON.stringify(graphInfo.figure.layout));

                        if (isDarkTheme) {
                            layout.paper_bgcolor = '#222529'; // Darkly theme background
                            layout.plot_bgcolor = '#222529';
                        }

                        // Increase font sizes for better readability in the saved image
                        const fontSizeIncrease = 6; // Increase font size by 6 points
                        if (layout.title) {
                            layout.title.font = layout.title.font || {};
                            layout.title.font.size = (layout.title.font.size || 16) + fontSizeIncrease;
                        }
                        ['xaxis', 'yaxis', 'yaxis2'].forEach(axis => {
                            if (layout[axis]) {
                                layout[axis].title = layout[axis].title || {};
                                layout[axis].title.font = layout[axis].title.font || {};
                                layout[axis].title.font.size = (layout[axis].title.font.size || 12) + fontSizeIncrease + 2;
                                layout[axis].tickfont = layout[axis].tickfont || {};
                                layout[axis].tickfont.size = (layout[axis].tickfont.size || 12) + fontSizeIncrease;
                            }
                        });
                        if (layout.legend) {
                            layout.legend.font = layout.legend.font || {};
                            layout.legend.font.size = (layout.legend.font.size || 10) + fontSizeIncrease + 2;
                        }

                        await window.Plotly.newPlot(tempDiv, data, layout, {displayModeBar: false});

                        const dataUrl = await window.Plotly.toImage(tempDiv, {
                            format: 'png',
                            height: originalGraphDiv.offsetHeight * 1.5,
                            width: originalGraphDiv.offsetWidth * 1.5,
                            scale: 2
                        });

                        const img = document.createElement('img');
                        img.src = dataUrl;
                        img.style.width = '100%';
                        img.style.height = 'auto';
                        graphDiv.parentNode.replaceChild(img, graphDiv);
                    } catch (e) {
                        console.error(`Plotly.toImage failed for ${graphInfo.id}:`, e);
                        const p = document.createElement('p');
                        p.innerText = '[Error converting chart to image]';
                        p.style.color = 'red';
                        graphDiv.parentNode.replaceChild(p, graphDiv);
                    } finally {
                        document.body.removeChild(tempDiv);
                    }
                } else if (graphDiv && graphDiv.parentNode) {
                    const p = document.createElement('p');
                    p.innerText = '[Chart not included in this report version.]';
                    graphDiv.parentNode.replaceChild(p, graphDiv);
                }
            }

            // --- Fetch and embed CSS ---
            let cssText = '';
            const styleSheets = Array.from(document.styleSheets);
            const cssPromises = styleSheets.map(sheet => {
                try {
                    // For external stylesheets, fetch the content
                    if (sheet.href) {
                        return fetch(sheet.href)
                            .then(response => response.ok ? response.text() : '')
                            .catch(() => '');
                    } else if (sheet.cssRules) {
                        return Promise.resolve(Array.from(sheet.cssRules).map(rule => rule.cssText).join('\\n'));
                    }
                } catch (e) {
                    // Silently fail on security errors for cross-origin stylesheets
                }
                return undefined; // Return undefined for sheets that can't be processed
            }).filter(p => p); // Filter out undefined promises

            const cssContents = await Promise.all(cssPromises);
            cssText = cssContents.join('\\n'); // Use '\\n' for JS newlines

            // --- Escape backticks and other problematic characters ---
            const cleanCssText = cssText.replace(/`/g, '\\`');

            // Construct the full HTML document
            const fullHtml = `
                <!DOCTYPE html>
                <html lang="en" data-bs-theme="${isDarkTheme ? 'dark' : 'light'}"><head><meta charset="utf-8"><title>Sync Progress Report</title><style>${cleanCssText}</style></head>
                <body>${clone.querySelector('body').innerHTML}</body>
                </html>
            `;

            return [fullHtml, {}];
        } catch (e) {
            alert('Caught an error in callback: ' + e.message);
            return ['CLIENTSIDE_ERROR: ' + e.message + '\\n' + e.stack, {}];
        }
    }
    """,
    [Output('html-content-store', 'data'),
     Output('save-callback-output', 'figure', allow_duplicate=True)],
    Input('save-button', 'n_clicks'),
    [State('ma-window-slider-progress', 'value'),
     State('progress-graph', 'figure'),
     State('blockheight-vs-speed-graph', 'figure'),
     State('distribution-graph', 'figure')],
    prevent_initial_call=True
)

@app.callback(
    [Output('action-feedback-store', 'data', allow_duplicate=True),
     Output('reports-filepath-store', 'data', allow_duplicate=True)],
    Input('html-content-store', 'data'),
    prevent_initial_call=True
)
def save_report_on_server(html_content):
    if not html_content or 'CLIENTSIDE_ERROR' in html_content:
        if html_content:
            return {'title': 'Error Saving Reports', 'body': "A client-side error occurred during report generation."}, None
        raise dash.exceptions.PreventUpdate
    # Generate a dynamic filename with timestamp
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"sync_progress_reports_{timestamp}.html"
    try:
        reports_dir = os.path.join(SCRIPT_DIR, "reports")
        os.makedirs(reports_dir, exist_ok=True)
        filepath = os.path.join(reports_dir, filename)
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(html_content)
        return {'title': 'Reports Saved', 'body': f"Reports successfully saved to: {filepath}"}, filepath
    except Exception as e:
        return {'title': 'Error Saving Reports', 'body': f"An error occurred while saving the file on the server: {e}"}, None

@app.callback(
    Output('reports-filepath-store', 'data', allow_duplicate=True),
    Input('open-report-button', 'n_clicks'),
    State('reports-filepath-store', 'data'),
    prevent_initial_call=True
)
def open_report_in_browser(n_clicks, filepath):
    if not n_clicks or not filepath:
        raise dash.exceptions.PreventUpdate
    try:
        webbrowser.open(f'file://{os.path.realpath(filepath)}')
    except Exception as e:
        print(f"Could not open report file: {e}")
        traceback.print_exc()
    return dash.no_update

if __name__ == "__main__":
    app.run(debug=True, port=8050)