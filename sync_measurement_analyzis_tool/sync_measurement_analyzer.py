import subprocess
import sys

def upgrade_dependencies():
    """Attempts to upgrade the core dependencies of the application at startup."""
    print("--- Checking for dashboard dependency updates ---")
    try:
        # Using a single call to pip is more efficient
        subprocess.run(
            [sys.executable, "-m", "pip", "install", "--upgrade", "dash", "dash-bootstrap-components", "pandas", "plotly"],
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
from plotly.subplots import make_subplots
import os
import webbrowser
import traceback
import re

import importlib.metadata

try:
    dash_version = importlib.metadata.version('dash')
    print(f"--- Diagnostic: Inside script, using Dash version: {dash_version} ---")
except importlib.metadata.PackageNotFoundError:
    print("--- Diagnostic: Dash package not found by importlib.metadata. ---")

def _monkey_patch_dash_for_loading_state(component_class):
    """
    A workaround for older or unusual Dash versions where 'loading_state' might not be a
    registered property on all components, causing validation errors. This
    dynamically adds the property to the component's list of known props.
    It's safe to run even on newer versions where the property already exists.
    """
    try:
        # In modern Dash versions, this is '_prop_names'. In some older or
        # custom versions, it might be 'prop_names' or '_children_props'.
        # We check for them in order of likelihood.
        prop_names_attr = None
        if hasattr(component_class, '_prop_names'):
            prop_names_attr = '_prop_names'
        elif hasattr(component_class, 'prop_names'):
            prop_names_attr = 'prop_names'
        elif hasattr(component_class, '_children_props'):
            prop_names_attr = '_children_props'
        
        if prop_names_attr:
            prop_names_tuple = getattr(component_class, prop_names_attr)
            if 'loading_state' not in prop_names_tuple:
                # The property list is a tuple, so we convert to a list to modify it.
                prop_names_list = list(prop_names_tuple)
                prop_names_list.append('loading_state')
                setattr(component_class, prop_names_attr, tuple(prop_names_list))
                print(f"Info: Monkey-patched {component_class.__name__} to support 'loading_state' using '{prop_names_attr}'.")
        else:
            print(f"Warning: Could not find a property list attribute on {component_class.__name__} to patch for 'loading_state'. The loading overlay might not work.")
    except Exception as e:
        print(f"Warning: Could not monkey-patch {component_class.__name__} for loading_state: {e}.")

# Patch dcc.Graph as it's used for ghost outputs to trigger the loading overlay.
_monkey_patch_dash_for_loading_state(dcc.Graph)

# --- SCRIPT DIRECTORY ---
# Use the real path to resolve any symlinks and get the directory of the script
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

# --- CSS and Asset Management ---
DROPDOWN_CSS = """
/*
  HIGH-SPECIFICITY DROPDOWN OVERRIDE
  Targets dropdowns by ID to ensure these styles have the highest priority.
  Includes both modern (Dash >= 2.0) and older (Dash < 2.0) selectors.
*/

/* --- Dropdown Control (the main input box) --- */
#start-block-dropdown .Select-control,
#end-block-dropdown .Select-control,
#start-block-dropdown .Select__control,
#end-block-dropdown .Select__control {
    background-color: var(--bs-body-bg) !important;
    border: 1px solid var(--bs-border-color) !important;
}

/* --- Text inside the dropdown (selected value, input) --- */
#start-block-dropdown .Select-value, 
#end-block-dropdown .Select-value,
#start-block-dropdown .Select-value-label, 
#end-block-dropdown .Select-value-label,
#start-block-dropdown .Select-input > input,
#end-block-dropdown .Select-input > input,
#start-block-dropdown .Select__single-value,
#end-block-dropdown .Select__single-value,
#start-block-dropdown .Select__input-container,
#end-block-dropdown .Select__input-container {
    color: var(--bs-body-color) !important;
}

/* --- The dropdown menu container --- */
#start-block-dropdown .Select-menu-outer,
#end-block-dropdown .Select-menu-outer,
#start-block-dropdown .Select__menu,
#end-block-dropdown .Select__menu {
    background-color: var(--bs-body-bg) !important;
    border: 1px solid var(--bs-border-color) !important;
}

/* --- Individual options in the dropdown --- */
#start-block-dropdown .Select-option,
#end-block-dropdown .Select-option,
#start-block-dropdown .Select__option,
#end-block-dropdown .Select__option {
    background-color: var(--bs-body-bg) !important;
    color: var(--bs-body-color) !important;
}

/* --- Focused option (hover) --- */
#start-block-dropdown .Select-option.is-focused,
#end-block-dropdown .Select-option.is-focused,
#start-block-dropdown .Select__option--is-focused,
#end-block-dropdown .Select__option--is-focused {
    background-color: var(--bs-primary) !important;
    color: white !important;
}

/* --- Selected option --- */
#start-block-dropdown .Select-option.is-selected,
#end-block-dropdown .Select-option.is-selected,
#start-block-dropdown .Select__option--is-selected,
#end-block-dropdown .Select__option--is-selected {
    background-color: var(--bs-secondary-bg) !important;
    color: var(--bs-body-color) !important; /* Ensure text is visible on selection */
}

/* --- Placeholder text --- */
#start-block-dropdown .Select--single > .Select-control .Select-placeholder,
#end-block-dropdown .Select--single > .Select-control .Select-placeholder,
#start-block-dropdown .Select__placeholder,
#end-block-dropdown .Select__placeholder {
    color: var(--bs-secondary-color) !important;
}

/* --- Arrow and Separator --- */
#start-block-dropdown .Select-arrow,
#end-block-dropdown .Select-arrow {
    border-color: var(--bs-body-color) transparent transparent !important;
}

#start-block-dropdown .Select__indicator-separator,
#end-block-dropdown .Select__indicator-separator {
    background-color: var(--bs-border-color) !important;
}

#start-block-dropdown .Select__indicator,
#end-block-dropdown .Select__indicator,
#start-block-dropdown .Select__dropdown-indicator,
#end-block-dropdown .Select__dropdown-indicator {
    color: var(--bs-secondary-color) !important;
}
"""

def setup_directories():
    """Creates necessary directories if they don't exist."""
    # Assets directory for CSS
    assets_dir = os.path.join(SCRIPT_DIR, "assets")
    if not os.path.isdir(assets_dir):
        os.makedirs(assets_dir)
    with open(os.path.join(assets_dir, "dropdown_styles.css"), "w", encoding="utf-8") as f:
        f.write(DROPDOWN_CSS)

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

initial_metadata = {}
initial_csv_path = os.path.join(SCRIPT_DIR, "measurements", "sync_measurement.csv")
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
        'title': 'Original sync_measurement.csv',
        'body': "This is the primary data file for analysis. It should be a CSV file containing detailed synchronization performance data, typically named `sync_measurement.csv`. The data from this file will be used to generate the main graphs and metrics. You can either drag and drop the file here or click to select it from your computer."
    },
    'Comparison File': {
        'title': 'Comparison sync_measurement.csv',
        'body': "This is an optional secondary data file used for comparison. If you provide a second `sync_measurement.csv` file here, the application will display its data alongside the original data, allowing for a side-by-side performance comparison. This is useful for evaluating the impact of changes in node configuration, hardware, or network conditions."
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
        'title': 'Total Sync in Progress Time',
        'body': "This metric represents the total duration the node has spent in an active synchronization state. The timer for this metric is active only when the node's block height is significantly behind the network's current height (typically more than 10 blocks). It provides a precise measure of the time required to catch up with the blockchain, excluding periods when the node is already fully synchronized or idle. This is a key performance indicator for evaluating the efficiency of the sync process under load."
    },
    'Total Blocks Synced': {
        'title': 'Total Blocks Synced',
        'body': "This value indicates the total number of blockchain blocks that have been downloaded, verified, and applied by the node during the measurement period. It is calculated as the difference between the final and initial block height. This metric, in conjunction with the sync time, is fundamental for calculating the overall synchronization speed."
    },
    'Total Transactions': {
        'title': 'Total Transactions',
        'body': "The total number of transactions processed within the selected block range."
    },
    'Overall Average Sync Speed (Blocks/sec)': {
        'title': 'Overall Average Sync Speed (Blocks/sec)',
        'body': "This is a high-level performance metric calculated by dividing the 'Total Blocks Synced' by the 'Total Sync in Progress Time' in seconds. It provides a single, averaged value representing the node's throughput over the entire synchronization process. While useful for a quick comparison, it can mask variations in performance that occur at different stages of syncing."
    },
    'Sync Speed (Blocks/sec sample)': {
        'title': 'Sync Speed (Blocks/sec) - Sampled Statistics',
        'body': "These metrics provide a statistical breakdown of the 'Blocks/sec' values calculated for each interval between data points.\n\n- Min: The minimum synchronization speed observed.\n- Q1/25th Percentile: The speed that the node meets or exceeds 75% of the time.\n- Mean: The statistical average of individual speed samples.\n- Median: The middle value of the performance data (50th percentile).\n- Q3/75th Percentile: The speed achieved during the faster sync periods.\n- Max: The peak synchronization speed achieved.\n- Std Dev: Measures the stability of the sync speed. A lower value is better.\n- Skewness: Measures the asymmetry of the performance data distribution."
    },
    'Push Block Time (ms)': {
        'title': 'Push Block Time (ms)',
        'body': "The total time taken to process and push a new block. This value is the sum of all individual timing components measured during block processing. The table shows statistical values (Min, Max, Avg, etc.) for this metric over the selected block range."
    },
    'Validation Time (ms)': {
        'title': 'Validation Time (ms)',
        'body': "The time spent on block-level validation, excluding the per-transaction validation loop. This is a CPU-intensive task. The table shows statistical values (Min, Max, Avg, etc.) for this metric over the selected block range."
    },
    'TX Loop Time (ms)': {
        'title': 'TX Loop Time (ms)',
        'body': "The time spent iterating through and validating all transactions within a block. This involves both CPU and database read operations. The table shows statistical values (Min, Max, Avg, etc.) for this metric over the selected block range."
    },
    'Housekeeping Time (ms)': {
        'title': 'Housekeeping Time (ms)',
        'body': "The time spent on various 'housekeeping' tasks during block processing, such as re-queuing unconfirmed transactions. The table shows statistical values (Min, Max, Avg, etc.) for this metric over the selected block range."
    },
    'TX Apply Time (ms)': {
        'title': 'TX Apply Time (ms)',
        'body': "The time spent applying the effects of each transaction within the block to the in-memory state. The table shows statistical values (Min, Max, Avg, etc.) for this metric over the selected block range."
    },
    'AT Time (ms)': {
        'title': 'AT Time (ms)',
        'body': "The time spent validating and processing all Automated Transactions (ATs) within the block. The table shows statistical values (Min, Max, Avg, etc.) for this metric over the selected block range."
    },
    'Subscription Time (ms)': {
        'title': 'Subscription Time (ms)',
        'body': "The time spent processing recurring subscription payments for the block. The table shows statistical values (Min, Max, Avg, etc.) for this metric over the selected block range."
    },
    'Block Apply Time (ms)': {
        'title': 'Block Apply Time (ms)',
        'body': "The time spent applying block-level changes, like distributing rewards and updating services. The table shows statistical values (Min, Max, Avg, etc.) for this metric over the selected block range."
    },
    'Commit Time (ms)': {
        'title': 'Commit Time (ms)',
        'body': "The time spent committing all in-memory state changes to the database on disk. This is a disk I/O-intensive operation. The table shows statistical values (Min, Max, Avg, etc.) for this metric over the selected block range."
    },
    'Misc Time (ms)': {
        'title': 'Misc Time (ms)',
        'body': "The time spent on miscellaneous, 'unaccounted for' calculations during block processing. The table shows statistical values (Min, Max, Avg, etc.) for this metric over the selected block range."
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
          'body': "This button filters the currently loaded CSV data by grouping it into chunks of 5000 blocks and calculating the average for most metrics within each chunk. This action reduces the dataset size in memory, which can improve performance and make long-term trends easier to see. The changes are not saved automatically; use the 'Save to New CSV' button to persist them."
          },
    'Average & Filter CSV': {
        'title': 'Average & Filter CSV',
        'body': "This button filters the currently loaded CSV data by grouping it into chunks of a selected size (1000-5000 blocks) and calculating the average for most metrics within each chunk. The genesis block (block 0) is always preserved. This action reduces the dataset size in memory, which can improve performance and make long-term trends easier to see. The changes are not saved automatically; use the 'Save to New CSV' button to persist them."
    },
    'Block Height': {
        'title': 'Block Height',
        'body': "The sequential number of a block in the blockchain. It represents a specific point in the history of the ledger. This table shows data sampled at various block heights."
    },
    'Sync Time (s)': {
        'title': 'Sync Time (seconds)',
        'body': "The total accumulated time in seconds that the node has spent in an active synchronization state up to this specific block height. This is the raw, unformatted value."
    },
    'Sync Time (Formatted)': {
        'title': 'Sync Time (Formatted)',
        'body': "A human-readable representation of the 'Sync Time (s)', formatted as Days-Hours:Minutes:Seconds. This makes it easier to understand longer synchronization durations."
    },
    'Sync Speed (Blocks/sec)': {
        'title': 'Sync Speed (Blocks/sec Instantaneous)',
        'body': "The instantaneous synchronization speed, calculated as the number of blocks processed since the last data point, divided by the time elapsed during that interval. This metric shows the node's performance at a specific moment and can fluctuate based on network conditions and block complexity."
    },
    'Save Filtered Range': {
        'title': 'Save Filtered Range',
        'body': "If checked, only the data within the currently selected block range (defined by the 'Start Block Height' and 'End Block Height' dropdowns) will be included in the saved CSV file. If unchecked, the entire dataset for the file will be saved, ignoring the block range filter."
    },
    'Save': {
        'title': 'Save',
        'body': "Saves the current state of the data (including any added or modified metadata and filtering/averaging) by overwriting the corresponding file in the `measurements/saved` directory. The filename will be kept consistent, without adding a new timestamp. Use this to update an existing saved file."
    },
    'Save As...': {
        'title': 'Save As...',
        'body': "Saves the current state of the data as a new CSV file in the `measurements/saved` directory. A timestamp and a unique sequence number will be appended to the filename to prevent overwriting previous saves. Use this to create a new version of the file while preserving the old one."
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

def average_df_by_chunks(df, chunk_size=5000):
    """Averages the dataframe in chunks of a given size, preserving the genesis block."""
    if df.empty or 'Block_height' not in df.columns:
        return df

    # Separate genesis block if it exists
    genesis_row = df[df['Block_height'] == 0]
    df_to_average = df[df['Block_height'] > 0].copy() # Use a copy to avoid side effects

    if df_to_average.empty:
        return genesis_row

    # Define aggregations for known columns
    aggregations = {
        'Block_timestamp': 'last',
        'Cumulative_difficulty': 'last',
        'Accumulated_sync_time[ms]': 'last',
        'Accumulated_sync_in_progress_time[ms]': 'last',
        'Push_block_time[ms]': 'mean',
        'Validation_time[ms]': 'mean',
        'Tx_loop_time[ms]': 'mean',
        'Housekeeping_time[ms]': 'mean',
        'Tx_apply_time[ms]': 'mean',
        'AT_time[ms]': 'mean',
        'Subscription_time[ms]': 'mean',
        'Block_apply_time[ms]': 'mean',
        'Commit_time[ms]': 'mean',
        'Misc_time[ms]': 'mean',
        'Transaction_count': 'sum',
        'Block_height': 'last' # Keep last block height to represent the chunk
    }

    # Filter out aggregations for columns that don't exist in the dataframe
    aggregations = {k: v for k, v in aggregations.items() if k in df_to_average.columns}

    if not aggregations:
        return genesis_row

    # Create a grouper column. Integer division does the chunking.
    # (df_to_average['Block_height'] - 1) ensures that blocks 1-5000 are in group 0, 5001-10000 in group 1 etc.
    df_to_average['group'] = ((df_to_average['Block_height'] - 1) // chunk_size)

    # Perform aggregation
    averaged_df = df_to_average.groupby('group').agg(aggregations).reset_index(drop=True)

    # Combine genesis row with the averaged data
    return pd.concat([genesis_row, averaged_df], ignore_index=True)

def create_combined_summary_table(df_original, df_compare, title_original, title_compare):
    """Creates a Dash component with a combined summary table of sync metrics."""

    def get_stats_dict(df):
        """Helper to calculate stats for a single dataframe, returning both display and raw values."""
        if df.empty or len(df) < 2:
            na_result = {'display': 'N/A', 'raw': None}
            return {
                'Total Time': na_result,
                'Total Sync in Progress Time': na_result, 'Total Blocks Synced': na_result, 'Total Transactions': na_result,
                'Overall Average Sync Speed (Blocks/sec)': na_result,
                'Min Sync Speed (Blocks/sec sample)': na_result, 'Q1 Sync Speed (Blocks/sec sample)': na_result,
                'Mean Sync Speed (Blocks/sec sample)': na_result, 'Median Sync Speed (Blocks/sec sample)': na_result,
                'Q3 Sync Speed (Blocks/sec sample)': na_result, 'Max Sync Speed (Blocks/sec sample)': na_result,
                'Std Dev of Sync Speed (Blocks/sec sample)': na_result, 'Skewness of Sync Speed (Blocks/sec sample)': na_result,
                'Avg Push Block Time (ms)': na_result, 'Avg Validation Time (ms)': na_result,
                'Avg TX Loop Time (ms)': na_result, 'Avg Housekeeping Time (ms)': na_result,
                'Avg TX Apply Time (ms)': na_result, 'Avg AT Time (ms)': na_result,
                'Avg Subscription Time (ms)': na_result, 'Avg Block Apply Time (ms)': na_result,
                'Avg Commit Time (ms)': na_result, 'Avg Misc Time (ms)': na_result
            }

        # BPS stats
        bps_series = df['Blocks_per_Second'].iloc[1:] if 'Blocks_per_Second' in df.columns else pd.Series(dtype=float)
        if bps_series.empty:
            stats = pd.Series(index=['min', '25%', 'mean', '50%', '75%', 'max', 'std'], dtype=float).fillna(0)
            skewness = 0.0
        else:
            stats = bps_series.describe(percentiles=[.25, .75])
            skewness = bps_series.skew()

        # Time and Block stats
        time_col = 'Accumulated_sync_in_progress_time[s]'

        # Total Sync in Progress Time
        total_sync_seconds = df[time_col].iloc[-1] - df[time_col].iloc[0] if time_col in df.columns else 0
        total_blocks_synced = df['Block_height'].iloc[-1] - df['Block_height'].iloc[0] if 'Block_height' in df.columns else 0
        overall_avg_bps = total_blocks_synced / total_sync_seconds if total_sync_seconds > 0 else 0.0
        total_transactions = df['Transaction_count'].sum() if 'Transaction_count' in df.columns else 0

        # Timing stats
        timing_cols = {
            'Push Block Time (ms)': 'Push_block_time[ms]',
            'Validation Time (ms)': 'Validation_time[ms]',
            'TX Loop Time (ms)': 'Tx_loop_time[ms]',
            'Housekeeping Time (ms)': 'Housekeeping_time[ms]',
            'TX Apply Time (ms)': 'Tx_apply_time[ms]',
            'AT Time (ms)': 'AT_time[ms]',
            'Subscription Time (ms)': 'Subscription_time[ms]',
            'Block Apply Time (ms)': 'Block_apply_time[ms]',
            'Commit Time (ms)': 'Commit_time[ms]',
            'Misc Time (ms)': 'Misc_time[ms]',
        }

        result = {
            'Total Sync in Progress Time': {'display': f"{format_seconds(total_sync_seconds)} ({int(total_sync_seconds)}s)", 'raw': total_sync_seconds},
            'Total Blocks Synced': {'display': f"{total_blocks_synced:,}", 'raw': float(total_blocks_synced)},
            'Total Transactions': {'display': f"{total_transactions:,}", 'raw': float(total_transactions)},
            'Overall Average Sync Speed (Blocks/sec)': {'display': f"{overall_avg_bps:.2f}", 'raw': overall_avg_bps},
            'HEADER_Sync Speed (Blocks/sec sample)': {'display': '', 'raw': None},
            'Min - Sync Speed (Blocks/sec sample)': {'display': f"{stats.get('min', 0):.2f}", 'raw': stats.get('min', 0.0)},
            'Q1 - Sync Speed (Blocks/sec sample)': {'display': f"{stats.get('25%', 0):.2f}", 'raw': stats.get('25%', 0.0)},
            'Mean - Sync Speed (Blocks/sec sample)': {'display': f"{stats.get('mean', 0):.2f}", 'raw': stats.get('mean', 0.0)},
            'Median - Sync Speed (Blocks/sec sample)': {'display': f"{stats.get('50%', 0):.2f}", 'raw': stats.get('50%', 0.0)},
            'Q3 - Sync Speed (Blocks/sec sample)': {'display': f"{stats.get('75%', 0):.2f}", 'raw': stats.get('75%', 0.0)},
            'Max - Sync Speed (Blocks/sec sample)': {'display': f"{stats.get('max', 0):.2f}", 'raw': stats.get('max', 0.0)},
            'Std Dev - Sync Speed (Blocks/sec sample)': {'display': f"{stats.get('std', 0):.2f}", 'raw': stats.get('std', 0.0)},
            'Skewness - Sync Speed (Blocks/sec sample)': {'display': f"{skewness:.2f}", 'raw': skewness if pd.notna(skewness) else 0.0}
        }
        for display_name, col_name in timing_cols.items():
            result[f'HEADER_{display_name}'] = {'display': '', 'raw': None}
            series = df[col_name] if col_name in df.columns and not df[col_name].empty else pd.Series(dtype=float)
            
            if series.empty:
                stats = pd.Series(index=['min', 'max', 'mean', '50%', 'std'], dtype=float).fillna(0)
            else:
                stats = series.describe(percentiles=[.5])

            result[f'Min - {display_name}'] = {'display': f"{stats.get('min', 0):.2f}", 'raw': stats.get('min', 0.0)}
            result[f'Max - {display_name}'] = {'display': f"{stats.get('max', 0):.2f}", 'raw': stats.get('max', 0.0)}
            result[f'Mean - {display_name}'] = {'display': f"{stats.get('mean', 0):.2f}", 'raw': stats.get('mean', 0.0)}
            result[f'Median - {display_name}'] = {'display': f"{stats.get('50%', 0):.2f}", 'raw': stats.get('50%', 0.0)}
            result[f'Std Dev - {display_name}'] = {'display': f"{stats.get('std', 0):.2f}", 'raw': stats.get('std', 0.0)}

        return result

    stats_original = get_stats_dict(df_original)
    has_comparison = not df_compare.empty

    metric_order = [
        'Total Sync in Progress Time', 'Total Blocks Synced', 'Total Transactions',
        'Overall Average Sync Speed (Blocks/sec)',
        'HEADER_Sync Speed (Blocks/sec sample)',
        'Min - Sync Speed (Blocks/sec sample)', 'Q1 - Sync Speed (Blocks/sec sample)',
        'Mean - Sync Speed (Blocks/sec sample)', 'Median - Sync Speed (Blocks/sec sample)',
        'Q3 - Sync Speed (Blocks/sec sample)', 'Max - Sync Speed (Blocks/sec sample)',
        'Std Dev - Sync Speed (Blocks/sec sample)', 'Skewness - Sync Speed (Blocks/sec sample)',
    ]
    timing_cols_keys = [
        'Push Block Time (ms)', 'Validation Time (ms)', 'TX Loop Time (ms)', 'Housekeeping Time (ms)',
        'TX Apply Time (ms)', 'AT Time (ms)', 'Subscription Time (ms)', 'Block Apply Time (ms)',
        'Commit Time (ms)', 'Misc Time (ms)'
    ]
    stats_keys = ['Min', 'Max', 'Mean', 'Median', 'Std Dev']
    for name in timing_cols_keys:
        metric_order.append(f'HEADER_{name}')
        for stat in stats_keys:
            metric_order.append(f'{stat} - {name}')
    
    metric_names = metric_order

    header_cells = [html.Th("Metric"), html.Th(title_original)]
    if has_comparison:
        stats_compare = get_stats_dict(df_compare)
        header_cells.append(html.Th(title_compare))
    
    table_header = [html.Thead(html.Tr(header_cells))]
    
    # Define which metrics are better when higher
    higher_is_better = {
        'Total Sync in Progress Time': False,
        'Total Blocks Synced': True,
        'Total Transactions': True,
        'Overall Average Sync Speed (Blocks/sec)': True,
        'Min - Sync Speed (Blocks/sec sample)': True,
        'Q1 - Sync Speed (Blocks/sec sample)': True,
        'Mean - Sync Speed (Blocks/sec sample)': True,
        'Median - Sync Speed (Blocks/sec sample)': True,
        'Q3 - Sync Speed (Blocks/sec sample)': True,
        'Max - Sync Speed (Blocks/sec sample)': True,
        'Std Dev - Sync Speed (Blocks/sec sample)': False,
        'Skewness - Sync Speed (Blocks/sec sample)': 'closer_to_zero',
    }
    for name in timing_cols_keys:
        for stat in stats_keys:
            metric_name = f'{stat} - {name}'
            higher_is_better[metric_name] = False # Lower is better for all timing stats

    table_body_rows = []
    for metric in metric_names:
        if metric.startswith('HEADER_'):
            header_text = metric.replace('HEADER_', '')
            info = tooltip_texts.get(header_text, {})
            title = info.get('title', header_text)
            info_icon = html.Span() # type: ignore
            if header_text in tooltip_texts:
                info_icon = html.Span([
                    " ",
                    html.I(className="bi bi-info-circle-fill text-info align-middle", style={'fontSize': '1.1em'}),
                ],
                    id={'type': 'info-icon', 'metric': header_text},
                    style={'cursor': 'pointer'},
                    title='Click for more info'
                )
            header_cell_content = [html.B(title), info_icon]
            col_span = 3 if has_comparison else 2
            table_body_rows.append(html.Tr([
                html.Td(header_cell_content, colSpan=col_span, style={'backgroundColor': 'var(--bs-body-bg)', 'paddingTop': '1rem'})
            ]))
            continue

        metric_parts = metric.split(' - ', 1)
        if len(metric_parts) > 1:
            metric_display_name = metric_parts[0]
            if metric_display_name == 'Mean':
                metric_display_name = 'Avg'
            metric_cell = html.Td(metric_display_name, style={'paddingLeft': '2em'})
        else:
            metric_display_name = metric
            metric_cell = html.Td(metric_display_name)
            
        # Original value cell
        original_val_display = stats_original.get(metric, {}).get('display', 'N/A')
        row_cells = [metric_cell, html.Td(original_val_display)]

        # Comparison value cell (with difference)
        if has_comparison:
            compare_val_display = stats_compare.get(metric, {}).get('display', 'N/A')
            compare_cell_content = [compare_val_display]

            # Calculate and display difference
            original_raw = stats_original.get(metric, {}).get('raw')
            compare_raw = stats_compare.get(metric, {}).get('raw')

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
                if metric in ['Total Blocks Synced', 'Total Transactions']:
                    diff_str = f"{diff:+,}"
                elif metric == 'Total Sync in Progress Time':
                    sign = "+" if diff > 0 else "-"
                    diff_str = f"{sign}{format_seconds(abs(diff))} ({sign}{int(abs(diff))}s)"
                else:
                    diff_str = f"{diff:+.2f}"

                if color_class:
                    compare_cell_content.append(html.Span(f" ({diff_str})", className=f"small {color_class} fw-bold"))

            row_cells.append(html.Td(compare_cell_content))

        table_body_rows.append(html.Tr(row_cells))

    table_body = [html.Tbody(table_body_rows)]

    return html.Div([
        html.H5("Metrics Summary", className="mt-4"),
        dbc.Table(table_header + table_body, striped=True, bordered=True, hover=True, className="mt-2")
    ])

def process_progress_df(df, filename=""):
    """Adds calculated columns to the measurement dataframe."""
    if df.empty:
        return df

    # For BPS calculation, we need the 'in_progress' time.
    # NOTE: Due to a bug in some node versions, for sync_measurement.csv,
    # the 'Accumulated_sync_time[ms]' column actually contains the "in-progress" time data,
    # while the header is swapped with 'Accumulated_sync_in_progress_time[ms]'.
    # This logic prioritizes the correct column for BPS calculation.
    
    time_col_s = 'Accumulated_sync_in_progress_time[s]'
    
    # This column has the correct 'in-progress' data due to the bug in sync_measurement.csv
    sync_in_progress_col_ms_bugged = 'Accumulated_sync_time[ms]'
    # This would be the correct column in a fixed sync_measurement.csv
    sync_in_progress_col_ms_correct = 'Accumulated_sync_in_progress_time[ms]'

    time_col = None

    if sync_in_progress_col_ms_bugged in df.columns:
        # Handle the bugged case: use the data from the 'total time' column for 'in-progress' calculations
        df[time_col_s] = df[sync_in_progress_col_ms_bugged] / 1000
        time_col = time_col_s
    elif sync_in_progress_col_ms_correct in df.columns:
        # Fallback for a potentially fixed file in the future.
        df[time_col_s] = df[sync_in_progress_col_ms_correct] / 1000
        time_col = time_col_s
    else:
        print(f"Warning: DataFrame from '{filename}' does not contain a recognized time column for sync speed calculation.")
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


# --- Dash App Initialization ---
app = dash.Dash(
    __name__,
    external_stylesheets=[
        dbc.themes.BOOTSTRAP,
        "https://cdn.jsdelivr.net/npm/bootstrap-icons@1.10.3/font/bootstrap-icons.css"
    ]
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
        html.Div("Loading...", id='loading-overlay-message', style={'color': 'var(--bs-body-color)'})
    ], style={
        'position': 'fixed', 'top': 0, 'left': 0, 'width': '100%', 'height': '100%',
        'display': 'none', 'justifyContent': 'center', 'alignItems': 'center',
        'flexDirection': 'column', 'backgroundColor': 'rgba(255, 255, 255, 0.5)',
        'zIndex': 10000,
    }),
    dcc.Store(id='tooltip-store', data=tooltip_texts),
    dcc.Store(id='reports-filepath-store'),
    html.Link(id="theme-stylesheet", rel="stylesheet"),
    dcc.Store(id='theme-store', storage_type='local'),
    dbc.Container([
    dcc.Graph(id='main-callback-output', style={'display': 'none'}),
    dcc.Graph(id='save-callback-output', style={'display': 'none'}),
    dcc.Graph(id='upload-callback-output', style={'display': 'none'}),
    dcc.Graph(id='average-callback-output', style={'display': 'none'}),
    dcc.Store(id='original-data-store', data=initial_original_data),
    dcc.Store(id='compare-data-store'), # No initial data for comparison
    dcc.Store(id='action-feedback-store'), # For modal feedback
    dcc.Store(id='unsaved-changes-store', data={'Original': {'changed': False, 'avg_chunk': None}, 'Comparison': {'changed': False, 'avg_chunk': None}}),
    dcc.Store(id='html-content-store'), # For saving HTML content
        dbc.Row([
        dbc.Col(html.H1("Sync Measurement Analyzer", className="mt-3 mb-4"), width="auto", className="me-auto"),
        dbc.Col([
            dbc.Button(html.I(className="bi bi-save"), id="save-button", color="secondary", className="me-3", title="Save Reports as HTML"),
            html.I(className="bi bi-sun-fill", style={'color': 'orange', 'fontSize': '1.2rem'}),
            dbc.Switch(id="theme-switch", value=True, className="d-inline-block mx-2"),
            html.I(className="bi bi-moon-stars-fill", style={'color': 'royalblue', 'fontSize': '1.2rem'}),
        ], width="auto", className="d-flex align-items-center mt-3")
    ], align="center"),

    dbc.Alert(
        "Original 'sync_measurement.csv' not found in 'measurements' folder. Please place it there or upload a file to begin.",
        id="no-file-alert",
        color="warning",
        is_open=initial_original_data is None,
    ),

    dbc.Row([
        dbc.Col([
            html.Div([
                html.Div(dcc.Upload(
                    id='upload-original-progress',
                    children=html.Div(['Drag and Drop or ', html.A('Select Original sync_measurement.csv')]),
                    style=upload_style,
                    multiple=False,
                ), style={'flexGrow': 1}),
                dbc.Button(html.I(className="bi bi-trash-fill"), id="discard-original-button", color="danger", outline=True, className="ms-2", style={'display': 'none'}, title="Discard this file"),
                html.Span([
                    " ",
                    html.I(className="bi bi-info-circle-fill text-info align-middle", style={'fontSize': '1.1em'}),
                ],
                    id={'type': 'info-icon', 'metric': 'Original File'}, # type: ignore
                    style={'cursor': 'pointer', 'marginLeft': '10px'},
                    title='Click for more info'
                )
            ], style={'display': 'flex', 'alignItems': 'center'}, id='original-upload-container'),
            html.Div(id='original-metadata-display', className="mt-3")
        ]),
        dbc.Col([
            html.Div([
                html.Div(dcc.Upload(
                    id='upload-compare-progress',
                    children=html.Div(['Drag and Drop or ', html.A('Select Comparison sync_measurement.csv')]),
                    style=upload_style,
                    multiple=False,
                ), style={'flexGrow': 1}),
                dbc.Button(html.I(className="bi bi-trash-fill"), id="discard-compare-button", color="danger", outline=True, className="ms-2", style={'display': 'none'}, title="Discard this file"),
                html.Span([
                    " ",
                    html.I(className="bi bi-info-circle-fill text-info align-middle", style={'fontSize': '1.1em'}),
                ],
                    id={'type': 'info-icon', 'metric': 'Comparison File'}, # type: ignore
                    style={'cursor': 'pointer', 'marginLeft': '10px'},
                    title='Click for more info'
                )
            ], style={'display': 'flex', 'alignItems': 'center'}, id='compare-upload-container'),
            html.Div(id='compare-metadata-display', className="mt-3")
        ]),
    ]),
    dbc.Row([
        dbc.Col([
            html.Label("Start Block Height:"),
            dcc.Dropdown(id='start-block-dropdown', clearable=False, placeholder="Upload data to select blocks")
        ], width=3),
        dbc.Col([
            html.Label("End Block Height:"),
            dcc.Dropdown(id='end-block-dropdown', clearable=False, placeholder="Upload data to select blocks")
        ], width=3),
        dbc.Col([
            html.Div([
                dbc.Button("Reset View", id="reset-view-button", color="secondary", className="w-100"),
                html.Span([
                    " ",
                    html.I(className="bi bi-info-circle-fill text-info align-middle", style={'fontSize': '1.1em'}),
                ], # type: ignore
                    id={'type': 'info-icon', 'metric': 'Reset View'},
                    style={'cursor': 'pointer', 'marginLeft': '10px'},
                    title='Click for more info'
                )
            ], style={'display': 'flex', 'alignItems': 'center'})
        ], width="auto", className="d-flex align-items-end"),
        dbc.Col(html.Div([
            dbc.InputGroup([
                dcc.Dropdown(
                    id='average-chunk-size-dropdown',
                    options=[{'label': f'{i} Blocks', 'value': i} for i in range(1000, 5001, 1000)],
                    value=5000,
                    clearable=False,
                    style={'width': '150px'}
                ),
                dbc.Button("Average", id="average-csv-button", color="warning"),
            ]),
            html.Span([
                " ",
                html.I(className="bi bi-info-circle-fill text-info align-middle", style={'fontSize': '1.1em'}),
            ], # type: ignore
                id={'type': 'info-icon', 'metric': 'Average & Filter CSV'},
                style={'cursor': 'pointer', 'marginLeft': '10px'},
                title='Click for more info'
            )
        ], style={'display': 'flex', 'alignItems': 'center'}), width="auto", className="d-flex align-items-end", id="average-csv-col", style={'display': 'none' if df_progress.empty else 'flex'}),
    ], className="mt-3"),
    html.Div([
        dcc.Graph(id="progress-graph"),
        html.Div([
            html.Label("Moving Average Window:", style={'marginRight': '5px'}),
            html.Span([
                " ",
                html.I(className="bi bi-info-circle-fill text-info align-middle", style={'fontSize': '1.1em'}),
            ], # type: ignore
            id={'type': 'info-icon', 'metric': 'Moving Average Window'},
            style={'cursor': 'pointer', 'marginRight': '10px'},
            title='Click for more info'
            ),
            html.Div(
                dcc.Slider(
                    id="ma-window-slider-progress",
                    min=0,
                    max=len(ma_windows) - 1,
                    value=1, # Default to 100
                    marks=ma_marks,
                    step=None,
                ), style={'width': '200px'}
            )
        ], style={'display': 'flex', 'alignItems': 'center', 'marginTop': '20px'}, id="ma-slider-container"),
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
     Output('upload-callback-output', 'figure', allow_duplicate=True)],
    Input('upload-original-progress', 'contents'),
    State('upload-original-progress', 'filename'),
    prevent_initial_call=True
)
def store_original_data(contents, filename):
    if not contents:
        return dash.no_update, dash.no_update, dash.no_update
    content_type, content_string = contents.split(',')
    decoded = base64.b64decode(content_string)
    try:
        decoded_str = decoded.decode('utf-8')
        lines = decoded_str.splitlines(True) # Keep newlines
        header_row = find_header_row(lines)
        metadata = extract_metadata(lines)
        # Pass only the data part of the file to pandas
        df = pd.read_csv(io.StringIO("".join(lines[header_row:])), sep=';')
        df.columns = df.columns.str.strip()

        # Check for essential columns
        if not any(col in df.columns for col in ['Accumulated_sync_time[ms]', 'Accumulated_sync_in_progress_time[ms]']) or 'Block_height' not in df.columns:
            raise ValueError("The uploaded file is missing essential columns like 'Block_height' and a recognized time column. Please check the file format.")

        store_data = {'filename': filename, 'data': df.to_json(date_format='iso', orient='split'), 'metadata': metadata}
        feedback = {'title': 'File Uploaded', 'body': f"Successfully loaded '{filename}'."}
        return store_data, feedback, {}
    except Exception as e:
        print(f"Error parsing original uploaded file: {e}")
        error_message = f"Failed to load '{filename}'.\n\nError: {e}\n\nPlease ensure it is a valid sync_measurement.csv file."
        feedback = {'title': 'Upload Failed', 'body': error_message}
        return None, feedback, {}

@app.callback(
    [Output('compare-data-store', 'data', allow_duplicate=True),
     Output('action-feedback-store', 'data', allow_duplicate=True),
     Output('upload-callback-output', 'figure', allow_duplicate=True)],
    Input('upload-compare-progress', 'contents'),
    State('upload-compare-progress', 'filename'),
    prevent_initial_call=True
)
def store_compare_data(contents, filename):
    if not contents:
        return dash.no_update, dash.no_update, dash.no_update
    content_type, content_string = contents.split(',')
    decoded = base64.b64decode(content_string)
    try:
        decoded_str = decoded.decode('utf-8')
        lines = decoded_str.splitlines(True) # Keep newlines
        header_row = find_header_row(lines)
        metadata = extract_metadata(lines)
        # Pass only the data part of the file to pandas
        df = pd.read_csv(io.StringIO("".join(lines[header_row:])), sep=';')
        df.columns = df.columns.str.strip()

        # Check for essential columns
        if not any(col in df.columns for col in ['Accumulated_sync_time[ms]', 'Accumulated_sync_in_progress_time[ms]']) or 'Block_height' not in df.columns:
            raise ValueError("The uploaded file is missing essential columns like 'Block_height' and a recognized time column. Please check the file format.")

        store_data = {'filename': filename, 'data': df.to_json(date_format='iso', orient='split'), 'metadata': metadata}
        feedback = {'title': 'File Uploaded', 'body': f"Successfully loaded '{filename}'."}
        return store_data, feedback, {}
    except Exception as e:
        print(f"Error parsing comparison uploaded file: {e}")
        error_message = f"Failed to load '{filename}'.\n\nError: {e}\n\nPlease ensure it is a valid sync_measurement.csv file."
        feedback = {'title': 'Upload Failed', 'body': error_message}
        return None, feedback, {}

# --- New callback for the clear button ---
@app.callback(
    [Output('original-data-store', 'data', allow_duplicate=True),
     Output('compare-data-store', 'data', allow_duplicate=True),
     Output('action-feedback-store', 'data', allow_duplicate=True),
     Output('unsaved-changes-store', 'data', allow_duplicate=True),
     Output('average-callback-output', 'figure', allow_duplicate=True)],
    Input('average-csv-button', 'n_clicks'),
    [State('original-data-store', 'data'),
     State('compare-data-store', 'data'),
     State('unsaved-changes-store', 'data'),
     State('average-chunk-size-dropdown', 'value')],
    prevent_initial_call=True
)
def average_csv_data(n_clicks, original_data, compare_data, unsaved_data, chunk_size):
    if not n_clicks:
        raise dash.exceptions.PreventUpdate

    feedback_messages = []
    unsaved_changes_made = False
    
    if original_data and 'data' in original_data:
        df_orig = pd.read_json(io.StringIO(original_data['data']), orient='split')
        rows_before = len(df_orig)
        df_orig_averaged = average_df_by_chunks(df_orig, chunk_size)
        rows_after = len(df_orig_averaged)
        original_data['data'] = df_orig_averaged.to_json(date_format='iso', orient='split')
        unsaved_data['Original']['changed'] = True
        unsaved_data['Original']['avg_chunk'] = chunk_size
        unsaved_changes_made = True
        feedback_messages.append(
            f"'{original_data.get('filename', 'Original file')}' averaged in memory from {rows_before:,} to {rows_after:,} rows using {chunk_size}-block chunks."
        )

    if compare_data and 'data' in compare_data:
        df_comp = pd.read_json(io.StringIO(compare_data['data']), orient='split')
        rows_before = len(df_comp)
        df_comp_averaged = average_df_by_chunks(df_comp, chunk_size)
        rows_after = len(df_comp_averaged)
        compare_data['data'] = df_comp_averaged.to_json(date_format='iso', orient='split')
        unsaved_data['Comparison']['changed'] = True
        unsaved_data['Comparison']['avg_chunk'] = chunk_size
        unsaved_changes_made = True
        feedback_messages.append(
            f"'{compare_data.get('filename', 'Comparison file')}' averaged in memory from {rows_before:,} to {rows_after:,} rows using {chunk_size}-block chunks."
        )

    if not unsaved_changes_made:
        feedback_body = "No data was loaded to average."
    else:
        feedback_messages.append("\nClick 'Save to New CSV' to persist these changes to a file.")
        feedback_body = feedback_messages

    feedback_data = {'title': 'CSV Data Averaged', 'body': feedback_body}

    return original_data, compare_data, feedback_data, unsaved_data, {}

@app.callback(
    Output('upload-original-progress', 'children'),
    Input('original-data-store', 'data')
)
def update_original_upload_text(data):
    """Updates the text of the original upload component based on whether data is loaded."""
    if data and data.get('filename'):
        return html.Div(f"Selected Original file: {data['filename']}", style={'wordBreak': 'break-all'})
    return html.Div(['Drag and Drop or ', html.A('Select Original sync_measurement.csv')])

@app.callback(
    Output('upload-compare-progress', 'children'),
    Input('compare-data-store', 'data')
)
def update_compare_upload_text(data):
    """Updates the text of the comparison upload component based on whether data is loaded."""
    if data and data.get('filename'):
        return html.Div(f"Selected Comparison file: {data['filename']}", style={'wordBreak': 'break-all'})
    return html.Div(['Drag and Drop or ', html.A('Select Comparison sync_measurement.csv')])

@app.callback(
    [Output('original-data-store', 'data', allow_duplicate=True),
     Output('upload-original-progress', 'contents', allow_duplicate=True)],
    Input('discard-original-button', 'n_clicks'),
    prevent_initial_call=True
)
def discard_original_data(n_clicks):
    """Clears the original data store and resets the upload component when the discard button is clicked."""
    if not n_clicks:
        return dash.no_update, dash.no_update
    # Setting store to None clears data, setting contents to None resets the Upload component
    return None, None

@app.callback(
    [Output('compare-data-store', 'data', allow_duplicate=True),
     Output('upload-compare-progress', 'contents', allow_duplicate=True)],
    Input('discard-compare-button', 'n_clicks'),
    prevent_initial_call=True
)
def discard_compare_data(n_clicks):
    """Clears the comparison data store and resets the upload component when the discard button is clicked."""
    if not n_clicks:
        return dash.no_update, dash.no_update
    # Setting store to None clears data, setting contents to None resets the Upload component
    return None, None

@app.callback(
    [Output('discard-original-button', 'style'),
     Output('discard-compare-button', 'style')],
    [Input('original-data-store', 'data'),
     Input('compare-data-store', 'data')]
)
def toggle_discard_buttons(original_data, compare_data):
    """Shows or hides the discard buttons based on whether data is loaded."""
    original_style = {'display': 'block'} if original_data else {'display': 'none'}
    compare_style = {'display': 'block'} if compare_data else {'display': 'none'}
    return original_style, compare_style

# --- Callback to Update Progress Graph ---
@app.callback(
    [Output("progress-graph", "figure"),
     Output("total-time-display-container", "children"),
     Output("start-block-dropdown", "options"),
     Output("start-block-dropdown", "value"),
     Output("end-block-dropdown", "options"),
     Output("end-block-dropdown", "value"),
     Output("data-table-container", "children"),
     Output("data-table-container", "style"),
     Output("main-callback-output", "figure"),
    ],
    [Input("ma-window-slider-progress", "value"),
     Input('original-data-store', 'data'),
     Input('compare-data-store', 'data'),
     Input('start-block-dropdown', 'value'),
     Input('end-block-dropdown', 'value'),
     Input('reset-view-button', 'n_clicks'),
     Input('show-data-table-switch', 'value'),
     Input('theme-store', 'data')],
)
def update_progress_graph_and_time(window_index, original_data, compare_data,
                                   start_block_val, end_block_val, reset_clicks,
                                   show_data_table, theme):
    window = ma_windows[window_index]
    ctx = dash.callback_context

    # --- Define colors based on theme ---
    is_dark_theme = theme != 'light'
    original_bps_color = '#b86e1e' if is_dark_theme else 'darkorange'  # Muted orange for dark theme
    original_ma_color = '#e0943b' if is_dark_theme else 'orange'      # Muted Amber for dark theme
    hover_label_style = dict(bgcolor="rgba(255, 255, 255, 0.8)", font=dict(color='black'))
    
    background_style = {}
    if is_dark_theme:
        hover_label_style = dict(bgcolor="rgba(34, 37, 41, 0.9)", font=dict(color='white'))
        background_style = {'paper_bgcolor': 'rgba(0,0,0,0)', 'plot_bgcolor': 'rgba(0,0,0,0)'}
 
    def create_header_with_tooltip(text, metric_id, style=None):
        if metric_id in tooltip_texts:
            info_icon = html.Span([
                " ",
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
    original_filename = "sync_measurement.csv" # Default filename
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
            title_text='Upload a sync_measurement.csv file to begin',
            template=graph_template,
            **background_style
        )
        summary_table = create_combined_summary_table(pd.DataFrame(), pd.DataFrame(), "Original", "Comparison")
        return empty_fig, summary_table, [], None, [], None, [], {'display': 'none'}

    # --- Process full dataframes first to get Blocks_per_Second ---
    if not df_progress_local.empty:
        df_progress_local = process_progress_df(df_progress_local, original_filename)
    if not df_compare.empty:
        df_compare = process_progress_df(df_compare, compare_filename)

    # --- Determine block range ---
    block_height_series = []
    if not df_progress_local.empty and 'Block_height' in df_progress_local.columns:
        block_height_series.append(df_progress_local['Block_height'])
    if not df_compare.empty and 'Block_height' in df_compare.columns:
        block_height_series.append(df_compare['Block_height'])

    if not block_height_series:
        graph_template = 'plotly_dark' if theme != 'light' else 'plotly'
        empty_fig = go.Figure()
        empty_fig.update_layout(
            title_text='No valid data with "Block_height" column found. Please check the uploaded files.',
            template=graph_template,
            **background_style
        )
        summary_table = create_combined_summary_table(pd.DataFrame(), pd.DataFrame(), "Original", "Comparison")
        return empty_fig, summary_table, [], None, [], None, [], {'display': 'none'}

    all_block_heights = pd.concat(block_height_series).dropna().unique()
    all_block_heights.sort()

    dropdown_options = [{'label': f"{int(h):,}", 'value': h} for h in all_block_heights]
    min_block = all_block_heights[0]
    max_block = all_block_heights[-1]

    triggered_id_str = ctx.triggered[0]['prop_id'].split('.')[0] if ctx.triggered else ''
    is_upload_or_clear = triggered_id_str in ['original-data-store', 'compare-data-store']
    is_reset = triggered_id_str == 'reset-view-button'

    start_block = min_block if is_upload_or_clear or is_reset or start_block_val is None else start_block_val
    end_block = max_block if is_upload_or_clear or is_reset or end_block_val is None else end_block_val

    if start_block > end_block:
        start_block, end_block = end_block, start_block

    # --- Filter dataframes for display and metrics ---
    df_original_display = df_progress_local[(df_progress_local['Block_height'] >= start_block) & (df_progress_local['Block_height'] <= end_block)].copy() if not df_progress_local.empty else pd.DataFrame()
    df_compare_display = df_compare[(df_compare['Block_height'] >= start_block) & (df_compare['Block_height'] <= end_block)].copy() if not df_compare.empty else pd.DataFrame()

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
                    '<b>Sync Time</b>: %{customdata[0]} (%{x:.0f}s)' +
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
                line=dict(color=original_bps_color, dash='dot', width=1),
                hovertemplate='<b>Sync Speed</b>: %{y:.2f} (Blocks/sec)<extra></extra>'
            ),
            secondary_y=True,
        )
        fig.add_trace(
            go.Scatter(
                x=df_original_display['Accumulated_sync_in_progress_time[s]'],
                y=df_original_display['BPS_ma'],
                name='Sync Speed (MA) (Original)',
                line=dict(color=original_ma_color, dash='solid'),
                hovertemplate='<b>Sync Speed (MA)</b>: %{y:.2f} (Blocks/sec)<extra></extra>'
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
                        '<b>Sync Time</b>: %{customdata[0]} (%{x:.0f}s)' +
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
                    hovertemplate='<b>Sync Speed</b>: %{y:.2f} (Blocks/sec)<extra></extra>'
                ),
                secondary_y=True,
            )
        fig.add_trace(
                go.Scatter(
                    x=df_compare_display['Accumulated_sync_in_progress_time[s]'],
                    y=df_compare_display['BPS_ma'],
                    name='Sync Speed (MA) (Comparison)',
                    line=dict(color='magenta', dash='solid'),
                    hovertemplate='<b>Sync Speed (MA)</b>: %{y:.2f} (Blocks/sec)<extra></extra>'
                ),
                secondary_y=True,
            )

    # Update layout and axes
    graph_template = 'plotly_dark' if theme != 'light' else 'plotly'
    fig.update_layout(
        title_text=f'Block Height vs. Sync Time (MA Window: {window})',
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
        margin=dict(l=80, r=0, t=80, b=80),
        **background_style
    )
    fig.update_yaxes(title_text="<b>Block Height</b>", secondary_y=False)
    fig.update_yaxes(title_text="<b>Sync Speed (Blocks/sec)</b>", secondary_y=True)
    fig.update_xaxes(title_text="Sync in Progress Time [s]")

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
        # Define all columns to show in the raw data table
        data_col_names = {
            'Accumulated_sync_in_progress_time[s]': 'Sync Time (s)',
            'SyncTime_Formatted': 'Sync Time (Formatted)',
            'Blocks_per_Second': 'Sync Speed (Blocks/sec)',
            'Transaction_count': 'TX Count',
            'Push_block_time[ms]': 'Push (ms)',
            'Validation_time[ms]': 'Validate (ms)',
            'Tx_loop_time[ms]': 'TX Loop (ms)',
            'Housekeeping_time[ms]': 'Housekeep (ms)',
            'Tx_apply_time[ms]': 'TX Apply (ms)',
            'AT_time[ms]': 'AT (ms)',
            'Subscription_time[ms]': 'Subscr. (ms)',
            'Block_apply_time[ms]': 'Block Apply (ms)',
            'Commit_time[ms]': 'Commit (ms)',
            'Misc_time[ms]': 'Misc (ms)',
        }
        cols_to_show = ['Block_height'] + list(data_col_names.keys())

        # Check if dataframes are valid for display (at least have Block_height)
        original_valid = not df_original_display.empty and 'Block_height' in df_original_display.columns
        compare_valid = not df_compare_display.empty and 'Block_height' in df_compare_display.columns
        
        if original_valid and compare_valid:
            # --- Merged Table Logic for Fixed Header ---
            # Dynamically select only the columns that are actually present in each dataframe
            orig_cols_present = [c for c in cols_to_show if c in df_original_display.columns]
            comp_cols_present = [c for c in cols_to_show if c in df_compare_display.columns]
            df_orig_subset = df_original_display[orig_cols_present].rename(columns=data_col_names)
            df_comp_subset = df_compare_display[comp_cols_present].rename(columns=data_col_names)

            df_merged = pd.merge(
                df_orig_subset,
                df_comp_subset,
                on='Block_height',
                how='outer',
                suffixes=('_orig', '_comp')
            ).sort_values(by='Block_height').reset_index(drop=True)
            df_merged.rename(columns={'Block_height': 'Block Height'}, inplace=True)

            # Determine the union of all display names to build a complete header, preserving order
            present_display_names = []
            for raw_name, display_name in data_col_names.items():
                if f"{display_name}_orig" in df_merged.columns or f"{display_name}_comp" in df_merged.columns:
                    present_display_names.append(display_name)

            # --- Build Header Table ---
            left_border_style = {'borderLeft': '1px solid black'}
            
            # --- Build Combined Header Table ---
            file_name_header_row = html.Tr([
                html.Th(""), # Spacer for Block Height
                html.Th(f"Original: {original_filename}", colSpan=len(present_display_names), className="text-center", style={'fontWeight': 'normal'}), # type: ignore
                html.Th(f"Comparison: {compare_filename}", colSpan=len(present_display_names), className="text-center", style={'fontWeight': 'normal', **left_border_style}) # type: ignore
            ])

            comparison_headers = [
                create_header_with_tooltip(col, col, style=left_border_style if i == 0 else None)
                for i, col in enumerate(present_display_names)
            ]
            column_name_header_row = html.Tr(
                [create_header_with_tooltip("Block Height", "Block Height")] +
                [create_header_with_tooltip(col, col) for col in present_display_names] +
                comparison_headers
            )

            header_table_children = [html.Thead([file_name_header_row, column_name_header_row])]
            header_table_style = {
                'tableLayout': 'fixed', 'width': '100%',
                'position': 'sticky', 'top': 0, 'zIndex': 2, # type: ignore
                'backgroundColor': 'var(--bs-body-bg, white)'
            }
            header_table = dbc.Table(header_table_children, bordered=True, className="mb-0", style=header_table_style)
            
            # --- Build Body Table ---
            body_rows = []
            for _, row in df_merged.iterrows():
                row_data = [html.Td(f"{int(row['Block Height']):,}" if pd.notna(row['Block Height']) else "")]
                
                # Add original columns' cells
                for display_name in present_display_names:
                    val = row.get(f"{display_name}_orig")
                    if pd.isna(val):
                        row_data.append(html.Td(""))
                    elif isinstance(val, float):
                        row_data.append(html.Td(f"{val:.2f}"))
                    else:
                        row_data.append(html.Td(val))

                # Add comparison columns' cells
                for i, display_name in enumerate(present_display_names):
                    cell_style = {'borderLeft': '1px solid black'} if i == 0 else {}
                    comp_val = row.get(f"{display_name}_comp")
                    
                    if pd.isna(comp_val):
                        row_data.append(html.Td("", style=cell_style)) # type: ignore
                    elif isinstance(comp_val, float):
                        row_data.append(html.Td(f"{comp_val:.2f}", style=cell_style)) # type: ignore
                    else:
                        row_data.append(html.Td(str(comp_val), style=cell_style)) # type: ignore

                body_rows.append(html.Tr(row_data))

            body_table = dbc.Table([html.Tbody(body_rows)], striped=True, bordered=True, hover=True, style={'tableLayout': 'fixed', 'width': '100%', 'marginTop': '-1px'})
            
            scrollable_div = html.Div(
                [header_table, body_table],
                style={'maxHeight': '500px', 'overflowY': 'auto', 'overflowX': 'auto'}
            )
            table_children.append(scrollable_div)

        elif original_valid:
            # Single table
            cols_present = [c for c in cols_to_show if c in df_original_display.columns]
            col_names = {'Block_height': 'Block Height', **data_col_names}
            df_orig_table = df_original_display[cols_present].rename(columns=col_names)

            table_children.append(html.H6(f"Original: {original_filename}"))
            header_table_style = {
                'tableLayout': 'fixed', 'width': '100%',
                'position': 'sticky', 'top': 0, 'zIndex': 2, # type: ignore
                'backgroundColor': 'var(--bs-body-bg, white)'
            }
            header_table = dbc.Table([html.Thead(html.Tr([create_header_with_tooltip(col, col) for col in df_orig_table.columns]))], bordered=True, className="mb-0", style=header_table_style)
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
            body_table = dbc.Table([html.Tbody(body_rows)], striped=True, bordered=True, hover=True, style={'tableLayout': 'fixed', 'width': '100%', 'marginTop': '-1px'})
            scrollable_div = html.Div(
                [header_table, body_table],
                style={'maxHeight': '500px', 'overflowY': 'auto', 'overflowX': 'auto'}
            )
            table_children.append(scrollable_div)

        elif compare_valid:
            # Single table for comparison data
            cols_present = [c for c in cols_to_show if c in df_compare_display.columns]
            col_names = {'Block_height': 'Block Height', **data_col_names}
            df_comp_table = df_compare_display[cols_present].rename(columns=col_names)

            table_children.append(html.H6(f"Comparison: {compare_filename}"))
            header_table_style = {
                'tableLayout': 'fixed', 'width': '100%',
                'position': 'sticky', 'top': 0, 'zIndex': 2, # type: ignore
                'backgroundColor': 'var(--bs-body-bg, white)'
            }
            header_table = dbc.Table([html.Thead(html.Tr([create_header_with_tooltip(col, col) for col in df_comp_table.columns]))], bordered=True, className="mb-0", style=header_table_style)
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
            body_table = dbc.Table([html.Tbody(body_rows)], striped=True, bordered=True, hover=True, style={'tableLayout': 'fixed', 'width': '100%', 'marginTop': '-1px'})
            scrollable_div = html.Div([header_table, body_table], style={'maxHeight': '500px', 'overflowY': 'auto', 'overflowX': 'auto'})
            table_children.append(scrollable_div)
        else:
            table_children = [html.P("No data to display in table.")]

    return fig, summary_table, dropdown_options, start_block, dropdown_options, end_block, table_children, table_style, {}

@app.callback(
    Output('action-feedback-store', 'data', allow_duplicate=True),
    Input('reset-view-button', 'n_clicks'),
    prevent_initial_call=True
)
def handle_reset_feedback(n_clicks):
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

    if triggered_id == "action-feedback-modal-close":
        return False, dash.no_update, dash.no_update, {'display': 'none'}

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

    return is_open, dash.no_update, dash.no_update, dash.no_update

@app.callback(
    Output({'type': 'unsaved-changes-badge', 'prefix': dash.dependencies.ALL}, 'style'),
    Input('unsaved-changes-store', 'data'),
    [State({'type': 'unsaved-changes-badge', 'prefix': dash.dependencies.ALL}, 'id')]
)
def update_unsaved_changes_badge(unsaved_data, ids):
    if not unsaved_data:
        return [dash.no_update] * len(ids)
    
    styles = []
    for component_id in ids:
        prefix = component_id['prefix']
        if unsaved_data.get(prefix, {}).get('changed', False):
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
        unsaved_data['Original']['changed'] = True # type: ignore
        return store_data, unsaved_data
    
    raise dash.exceptions.PreventUpdate

@app.callback(
    Output('original-data-store', 'data', allow_duplicate=True),
    Output('unsaved-changes-store', 'data', allow_duplicate=True),
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
        unsaved_data['Original']['changed'] = True
        return store_data, unsaved_data
    
    raise dash.exceptions.PreventUpdate

@app.callback(
    Output('compare-data-store', 'data', allow_duplicate=True),
    Output('unsaved-changes-store', 'data', allow_duplicate=True),
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
        unsaved_data['Comparison']['changed'] = True
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
        unsaved_data['Comparison']['changed'] = True # type: ignore
        return store_data, unsaved_data
    
    raise dash.exceptions.PreventUpdate

def write_csv_new(store_data, filter_range, start_block, end_block, avg_chunk_size=None):
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
    if avg_chunk_size:
        suffix += f"_averaged_{avg_chunk_size}"
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
        saved_dir = os.path.join(SCRIPT_DIR, "measurements", "saved")
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
        base = re.sub(r'_averaged_\d+$', '', base) # averaged suffix

        # Construct the new filename
        new_base_filename = f"{base}{suffix}{hostname_part}_{timestamp}"
        
        # Find a unique filename by appending a counter if necessary
        filepath = os.path.join(saved_dir, f"{new_base_filename}.csv")
        counter = 1
        while os.path.exists(filepath):
            filepath = os.path.join(saved_dir, f"{new_base_filename}_{counter}.csv")
            counter += 1
        
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(output.getvalue()) # type: ignore
        
        return f"File with updated data saved to: {filepath}"
    except Exception as e:
        return f"Error saving file: {e}"

def write_csv_overwrite(store_data, filter_range, start_block, end_block, avg_chunk_size=None):
    if not store_data:
        return "No data in store to save."

    filename = store_data.get('filename', 'unknown_file.csv')
    metadata = store_data.get('metadata', {})
    df_json = store_data.get('data')

    if not df_json:
        return f"No data content found for '{filename}'."

    df = pd.read_json(io.StringIO(df_json), orient='split')

    suffix = ""
    if avg_chunk_size:
        suffix += f"_averaged_{avg_chunk_size}"
    if filter_range and start_block is not None and end_block is not None:
        df = df[(df['Block_height'] >= start_block) & (df['Block_height'] <= end_block)]
        suffix += f"_range_{int(start_block)}-{int(end_block)}"

    output = io.StringIO()
    if metadata:
        output.write("Property;Value\n")
        for key, value in metadata.items():
            output.write(f"{key};{value}\n")
        output.write(";;\n")

    df.to_csv(output, sep=';', index=False)

    try:
        saved_dir = os.path.join(SCRIPT_DIR, "measurements", "saved")
        os.makedirs(saved_dir, exist_ok=True)
        base_filename = os.path.basename(filename)
        base, ext = os.path.splitext(base_filename)

        # Clean up old suffixes
        base = re.sub(r'_\d+$', '', base)
        base = re.sub(r'_\d{8}_\d{6}$', '', base)
        base = re.sub(r'_hostname_[\w\.\-]+$', '', base)
        base = re.sub(r'_range_\d+-\d+$', '', base)
        base = re.sub(r'_averaged_\d+$', '', base)

        hostname_part = ""
        hostname = metadata.get('Hostname')
        if hostname:
            sanitized_hostname = re.sub(r'[^\w\.\-]', '_', hostname)
            hostname_part = f"_hostname_{sanitized_hostname}"

        new_filename = f"{base}{suffix}{hostname_part}.csv"
        filepath = os.path.join(saved_dir, new_filename)

        with open(filepath, "w", encoding="utf-8") as f:
            f.write(output.getvalue()) # type: ignore
        
        return f"File saved to: {filepath}"
    except Exception as e:
        return f"Error saving file: {e}"

@app.callback(
    [Output('action-feedback-store', 'data', allow_duplicate=True),
     Output('unsaved-changes-store', 'data', allow_duplicate=True)],
    [Input({'type': 'save-as-button', 'prefix': dash.dependencies.ALL}, 'n_clicks'),
     Input({'type': 'save-overwrite-button', 'prefix': dash.dependencies.ALL}, 'n_clicks')],
    [State('original-data-store', 'data'), 
     State('compare-data-store', 'data'),
     State({'type': 'save-filter-range-check', 'prefix': dash.dependencies.ALL}, 'value'),
     State('start-block-dropdown', 'value'),
     State('end-block-dropdown', 'value'),
     State('unsaved-changes-store', 'data')],
    prevent_initial_call=True
)
def save_csv(n_clicks_as, n_clicks_overwrite, original_data, compare_data, filter_range_values, start_block, end_block, unsaved_data):
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

    message = "An unknown error occurred."
    avg_chunk = unsaved_data.get(prefix, {}).get('avg_chunk')
    data_to_save = original_data if prefix == 'Original' else compare_data

    if data_to_save:
        if save_as:
            message = write_csv_new(data_to_save, filter_range, start_block, end_block, avg_chunk)
        else:
            message = write_csv_overwrite(data_to_save, filter_range, start_block, end_block, avg_chunk)
        
        if 'Error' not in message:
            unsaved_data[prefix] = {'changed': False, 'avg_chunk': None}

    return {'title': 'Save to CSV', 'body': message}, unsaved_data

@app.callback(
    Output('theme-switch', 'value'),
    Output('theme-stylesheet', 'href'),
    Input('theme-store', 'data'),
)
def load_initial_theme(stored_theme):
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

@app.callback(
    [Output('original-metadata-display', 'children'),
     Output('compare-metadata-display', 'children')],
    [Input('original-data-store', 'data'),
     Input('compare-data-store', 'data')]
)
def update_metadata_display(original_data, compare_data):
    def create_metadata_card(data, title_prefix):
        if not data:
            return None

        metadata = data.get('metadata', {})
        filename = data.get('filename', 'data file')

        card_header = dbc.CardHeader(
            dbc.Row([
                dbc.Col(f"{title_prefix} System Info: {os.path.basename(filename)}", className="fw-bold"),
                dbc.Col([
                    dbc.Badge("Unsaved", color="warning", className="me-2", id={'type': 'unsaved-changes-badge', 'prefix': title_prefix}, style={'display': 'none', 'verticalAlign': 'middle'}),
                    dcc.Checklist(
                        options=[{'label': ' Filter range', 'value': 'filter'}],
                        value=[],
                        id={'type': 'save-filter-range-check', 'prefix': title_prefix},
                        inline=True,
                        className="me-1"
                    ),
                    html.Span([
                        " ",
                        html.I(className="bi bi-info-circle-fill text-info align-middle", style={'fontSize': '1.1em'}),
                    ], id={'type': 'info-icon', 'metric': 'Save Filtered Range'}, style={'cursor': 'pointer'}, title='Click for more info'),
                    html.Div("|", className="text-muted mx-2"),
                    dbc.Button("Save", id={'type': 'save-overwrite-button', 'prefix': title_prefix}, size="sm", color="primary", className="me-1"),
                    html.Span([
                        " ",
                        html.I(className="bi bi-info-circle-fill text-info align-middle", style={'fontSize': '1.1em'}),
                    ], id={'type': 'info-icon', 'metric': 'Save'}, style={'cursor': 'pointer'}, title='Click for more info'),
                    html.Div("|", className="text-muted mx-2"),
                    dbc.Button("Save As...", id={'type': 'save-as-button', 'prefix': title_prefix}, size="sm", color="success", className="me-1"),
                    html.Span([
                        " ",
                        html.I(className="bi bi-info-circle-fill text-info align-middle", style={'fontSize': '1.1em'}),
                    ], id={'type': 'info-icon', 'metric': 'Save As...'}, style={'cursor': 'pointer'}, title='Click for more info'),
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
                info_icon = html.Span([
                    " ",
                    html.I(className="bi bi-info-circle-fill text-info align-middle", style={'fontSize': '1.1em'}),
                ], id={'type': 'info-icon', 'metric': unique_metric_id}, style={'cursor': 'pointer', 'marginLeft': '5px'}, title='Click for more info')
                key_with_icon.append(info_icon)

            return dbc.ListGroupItem(
                [
                    html.Div(key_with_icon, style={'display': 'flex', 'alignItems': 'center'}),
                    dbc.Input(
                        id={'type': 'metadata-input', 'prefix': title_prefix, 'key': key},
                        value=str(value),
                        type='text',
                        className="text-end text-muted",
                        size="sm",
                        style={'border': 'none', 'backgroundColor': 'transparent', 'boxShadow': 'none', 'padding': '0', 'margin': '0', 'height': 'auto'},
                        debounce=True
                    )
                ],
                className="d-flex justify-content-between p-2"
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

        # --- New UI elements for adding metadata ---
        add_metadata_form = dbc.ListGroupItem([
            dbc.Row([
                dbc.Col(dcc.Input(id={'type': 'metadata-key-input', 'prefix': title_prefix}, placeholder='New Property', type='text', className='form-control form-control-sm'), width=4),
                dbc.Col(dcc.Input(id={'type': 'metadata-value-input', 'prefix': title_prefix}, placeholder='Value', type='text', className='form-control form-control-sm'), width=5),
                dbc.Col(dbc.Button("Add", id={'type': 'add-metadata-button', 'prefix': title_prefix}, color="primary", size="sm", className="w-100"), width=3)
            ], align="center", className="g-2") # g-2 for gutter
        ], className="p-2")

        list_group_items.append(add_metadata_form)

        card_body = dbc.ListGroup(list_group_items, flush=True)
        return dbc.Card([card_header, card_body])

    original_card = create_metadata_card(original_data, "Original")
    compare_card = create_metadata_card(compare_data, "Comparison")

    return original_card, compare_card

# --- Callbacks for saving HTML report ---
app.clientside_callback(
    """
    async function(n_clicks, slider_value_index, figure) {
        if (!n_clicks) {
            return [window.dash_clientside.no_update, window.dash_clientside.no_update];
        }
        if (!figure) {
            return [window.dash_clientside.no_update, window.dash_clientside.no_update];
        }
        try {
            const mainContainer = document.getElementById('main-container');
            if (!mainContainer) {
                return ['CLIENTSIDE_ERROR: main-container element not found.', null];
            }
            const clone = mainContainer.cloneNode(true);
            const isDarkTheme = document.documentElement.getAttribute('data-bs-theme') === 'dark';
            const dropdownIds = ['start-block-dropdown', 'end-block-dropdown'];
            dropdownIds.forEach(id => {
                const originalDropdownValue = document.querySelector(`#${id} .Select-value-label, #${id} .Select__single-value`);
                const clonedDropdown = clone.querySelector(`#${id}`);
                if (clonedDropdown && clonedDropdown.parentNode) {
                    const valueText = originalDropdownValue ? originalDropdownValue.textContent : 'N/A';
                    const staticEl = document.createElement('div');
                    staticEl.textContent = valueText;
                    staticEl.style.cssText = `
                        margin-top: 0px; padding: 6px 12px; border-radius: 4px; font-size: 0.9rem;
                        background-color: var(--bs-body-bg); border: 1px solid var(--bs-border-color); color: var(--bs-body-color);
                    `;
                    clonedDropdown.parentNode.replaceChild(staticEl, clonedDropdown);
                }
            });
            const ma_windows = [10, 100, 200, 300, 400, 500];
            const window_size = ma_windows[slider_value_index];
            const sliderContainer = clone.querySelector('#ma-slider-container');
            if (sliderContainer) {
                const staticEl = document.createElement('div');
                staticEl.textContent = `Moving Average Window: ${window_size}`;
                staticEl.className = 'mt-3'; staticEl.style.fontWeight = 'bold';
                sliderContainer.parentNode.replaceChild(staticEl, sliderContainer);
            }
            const graphDiv = clone.querySelector('#progress-graph');
            const originalGraphDiv = document.getElementById('progress-graph');
            if (graphDiv && originalGraphDiv && window.Plotly) {
                const tempDiv = document.createElement('div');
                tempDiv.style.position = 'absolute'; tempDiv.style.left = '-9999px';
                tempDiv.style.width = originalGraphDiv.offsetWidth + 'px';
                tempDiv.style.height = originalGraphDiv.offsetHeight + 'px';
                document.body.appendChild(tempDiv);
                try {
                    const data = JSON.parse(JSON.stringify(figure.data));
                    const layout = JSON.parse(JSON.stringify(figure.layout));
                    if (isDarkTheme) {
                        layout.paper_bgcolor = '#222529'; layout.plot_bgcolor = '#222529';
                    }
                    const fontSizeIncrease = 6;
                    if (layout.title) { layout.title.font = layout.title.font || {}; layout.title.font.size = (layout.title.font.size || 16) + fontSizeIncrease; }
                    if (layout.xaxis) { layout.xaxis.title = layout.xaxis.title || {}; layout.xaxis.title.font = layout.xaxis.title.font || {}; layout.xaxis.title.font.size = (layout.xaxis.title.font.size || 12) + fontSizeIncrease + 2; layout.xaxis.tickfont = layout.xaxis.tickfont || {}; layout.xaxis.tickfont.size = (layout.xaxis.tickfont.size || 12) + fontSizeIncrease; }
                    if (layout.yaxis) { layout.yaxis.title = layout.yaxis.title || {}; layout.yaxis.title.font = layout.yaxis.title.font || {}; layout.yaxis.title.font.size = (layout.yaxis.title.font.size || 12) + fontSizeIncrease + 2; layout.yaxis.tickfont = layout.yaxis.tickfont || {}; layout.yaxis.tickfont.size = (layout.yaxis.tickfont.size || 12) + fontSizeIncrease; }
                    if (layout.yaxis2) { layout.yaxis2.title = layout.yaxis2.title || {}; layout.yaxis2.title.font = layout.yaxis2.title.font || {}; layout.yaxis2.title.font.size = (layout.yaxis2.title.font.size || 12) + fontSizeIncrease + 2; layout.yaxis2.tickfont = layout.yaxis2.tickfont || {}; layout.yaxis2.tickfont.size = (layout.yaxis2.tickfont.size || 12) + fontSizeIncrease; }
                    if (layout.legend) { layout.legend.font = layout.legend.font || {}; layout.legend.font.size = (layout.legend.font.size || 10) + fontSizeIncrease + 2; }
                    await window.Plotly.newPlot(tempDiv, data, layout);
                    const renderWidth = originalGraphDiv.offsetWidth * 1.4;
                    const renderHeight = originalGraphDiv.offsetHeight * 1.4;
                    const dataUrl = await window.Plotly.toImage(tempDiv, { format: 'png', height: renderHeight, width: renderWidth, scale: 2 });
                    const img = document.createElement('img');
                    img.src = dataUrl;
                    img.style.width = '150%'; img.style.position = 'relative'; img.style.left = '-25%'; img.style.height = 'auto';
                    graphDiv.parentNode.replaceChild(img, graphDiv);
                } catch (e) {
                    console.error('Plotly.toImage failed:', e);
                    const p = document.createElement('p'); p.innerText = '[Error converting chart to image]'; p.style.color = 'red';
                    graphDiv.parentNode.replaceChild(p, graphDiv);
                } finally {
                    document.body.removeChild(tempDiv);
                }
            }
            let cssText = '';
            const styleSheets = Array.from(document.styleSheets);
            const cssPromises = styleSheets.map(sheet => {
                try {
                    if (sheet.href) {
                        return fetch(sheet.href).then(response => response.ok ? response.text() : '').catch(() => '');
                    } else if (sheet.cssRules) {
                        let rules = '';
                        for (let i = 0; i < sheet.cssRules.length; i++) { rules += sheet.cssRules[i].cssText; }
                        return Promise.resolve(rules);
                    }
                } catch (e) {}
                return Promise.resolve('');
            });
            const cssContents = await Promise.all(cssPromises);
            cssText = cssContents.join('\\n');
            const cleanCssText = cssText.replace(/`/g, '\\`');
            const elementsToRemove = clone.querySelectorAll('#save-button, #theme-switch, .bi-sun-fill, .bi-moon-stars-fill, #clear-csv-button, #reset-view-button, #show-data-table-switch-container, #original-upload-container, #compare-upload-container, span[id*="info-icon"]');
            elementsToRemove.forEach(el => el.parentNode.removeChild(el));
            const cleanOuterHtml = clone.outerHTML.replace(/`/g, '\\`');
            const fullHtml = `
                <!DOCTYPE html><html lang="en"><head><meta charset="utf-8"><title>Sync Measurement Reports</title>
                <style>${cleanCssText} body { font-family: sans-serif; } .container-fluid { max-width: 1200px !important; margin: 0 auto !important; padding: 20px !important; float: none !important; } #save-button, #theme-switch, .bi-sun-fill, .bi-moon-stars-fill, #ma-slider-container, #clear-csv-button, #reset-view-button, #show-data-table-switch-container, #original-upload-container, #compare-upload-container, span[id*="info-icon"] { display: none !important; } body, body * { -webkit-user-select: text !important; -moz-user-select: text !important; -ms-user-select: text !important; user-select: text !important; }</style>
                </head><body>${cleanOuterHtml}</body></html>
            `;
            return [fullHtml, null];
        } catch (e) {
            alert('Caught an error in callback: ' + e.message);
            const error_content = 'CLIENTSIDE_ERROR: ' + e.message + '\\n' + e.stack;
            return [error_content, ""];
        }
    }
    """,
    [Output('html-content-store', 'data'),
     Output('save-callback-output', 'figure')],
    Input('save-button', 'n_clicks'),
    [State('ma-window-slider-progress', 'value'),
     State('progress-graph', 'figure')],
    prevent_initial_call=True
)

@app.callback(
    [Output('action-feedback-store', 'data', allow_duplicate=True),
     Output('reports-filepath-store', 'data')],
    Input('html-content-store', 'data'),
    prevent_initial_call=True
)
def save_report_on_server(html_content):
    if not html_content or 'CLIENTSIDE_ERROR' in html_content:
        if html_content:
            return {'title': 'Error Saving Reports', 'body': "A client-side error occurred during report generation."}, None
        raise dash.exceptions.PreventUpdate
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"sync_measurement_reports_{timestamp}.html"
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

app.clientside_callback(
    """
    function(main_loading_state, save_loading_state, upload_loading_state, average_loading_state, theme) {
        const ctx = dash_clientside.callback_context;
        if (!ctx.triggered.length) {
            return [window.dash_clientside.no_update, window.dash_clientside.no_update];
        }

        const main_loading = main_loading_state && main_loading_state.is_loading;
        const save_loading = save_loading_state && save_loading_state.is_loading;
        const upload_loading = upload_loading_state && upload_loading_state.is_loading;
        const average_loading = average_loading_state && average_loading_state.is_loading;

        const style = {
            'position': 'fixed', 'top': 0, 'left': 0, 'width': '100%', 'height': '100%',
            'justifyContent': 'center', 'alignItems': 'center', 'flexDirection': 'column',
            'zIndex': 10000
        };

        if (main_loading || save_loading || upload_loading || average_loading) {
            let message = "Loading...";
            const trigger_id = ctx.triggered[0].prop_id.split('.')[0];
            if (trigger_id === 'main-callback-output') {
                message = "Recalculating metrics and updating view...";
            } else if (trigger_id === 'save-callback-output') {
                message = "Generating HTML report...";
            } else if (trigger_id === 'upload-callback-output') {
                message = "Loading and processing CSV file...";
            } else if (trigger_id === 'average-callback-output') {
                message = "Calculating averages and filtering data...";
            }
            
            const isDarkTheme = theme === 'dark';
            const bgColor = isDarkTheme ? 'rgba(34, 37, 41, 0.8)' : 'rgba(255, 255, 255, 0.8)';
            
            style.display = 'flex';
            style.backgroundColor = bgColor;

            return [style, message];
        }
        
        style.display = 'none';
        return [style, window.dash_clientside.no_update];
    }
    """,
    [Output('loading-overlay', 'style'),
     Output('loading-overlay-message', 'children')],
    [Input('main-callback-output', 'loading_state'),
     Input('save-callback-output', 'loading_state'), # This should be a dcc.Graph
     Input('upload-callback-output', 'loading_state'), # This should be a dcc.Graph
     Input('average-callback-output', 'loading_state'), # This should be a dcc.Graph
     Input('theme-store', 'data')],
    prevent_initial_call=True
)

if __name__ == "__main__":
    app.run(debug=True, port=8051)