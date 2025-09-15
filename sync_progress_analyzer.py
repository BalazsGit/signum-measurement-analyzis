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

def setup_assets_folder():
    """Creates the assets folder and the required CSS file if they don't exist."""
    assets_dir = "assets"
    if not os.path.isdir(assets_dir):
        os.makedirs(assets_dir)
    with open(os.path.join(assets_dir, "dropdown_styles.css"), "w", encoding="utf-8") as f:
        f.write(DROPDOWN_CSS)

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
initial_csv_path = os.path.join("measurements", "sync_progress.csv")
try:
    with open(initial_csv_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    header_row = find_header_row(lines)
    initial_metadata = extract_metadata(lines)
    # Pass only the relevant lines to pandas, starting from the header
    df_progress = pd.read_csv(io.StringIO("".join(lines[header_row:])), sep=";")
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
        'title': 'Total Sync in Progress Time',
        'body': "This metric represents the total duration the node has spent in an active synchronization state. The timer for this metric is active only when the node's block height is significantly behind the network's current height (typically more than 10 blocks). It provides a precise measure of the time required to catch up with the blockchain, excluding periods when the node is already fully synchronized or idle. This is a key performance indicator for evaluating the efficiency of the sync process under load."
    },
    'Total Blocks Synced': {
        'title': 'Total Blocks Synced',
        'body': "This value indicates the total number of blockchain blocks that have been downloaded, verified, and applied by the node during the measurement period. It is calculated as the difference between the final and initial block height. This metric, in conjunction with the sync time, is fundamental for calculating the overall synchronization speed."
    },
    'Overall Average Sync Speed (Blocks/sec)': {
        'title': 'Overall Average Sync Speed (Blocks/sec)',
        'body': "This is a high-level performance metric calculated by dividing the 'Total Blocks Synced' by the 'Total Sync in Progress Time' in seconds. It provides a single, averaged value representing the node's throughput over the entire synchronization process. While useful for a quick comparison, it can mask variations in performance that occur at different stages of syncing."
    },
    'Min Sync Speed (Blocks/sec sample)': {
        'title': 'Min Sync Speed (Blocks/sec Sampled)',
        'body': "This metric shows the minimum synchronization speed observed between any two consecutive data points in the sample. A very low or zero value can indicate periods of network latency, slow peer response, or high computational load on the node (e.g., during verification of blocks with many complex transactions), causing a temporary stall in progress."
    },
    'Q1 Sync Speed (Blocks/sec sample)': {
        'title': 'Q1/25th Percentile Sync Speed (Blocks/sec)',
        'body': "The 25th percentile (or first quartile, Q1) is the value below which 25% of the synchronization speed samples fall. It indicates the performance level that the node meets or exceeds 75% of the time. A higher Q1 value suggests that the node consistently maintains a good minimum performance level, with fewer periods of very low speed."
    },
    'Mean Sync Speed (Blocks/sec sample)': {
        'title': 'Mean Sync Speed (Blocks/sec Sampled)',
        'body': "This represents the statistical average (mean) of the 'Blocks/sec' values calculated for each interval between data points. Unlike the 'Overall Average', this metric is the average of individual speed samples, giving a more granular view of the typical performance, but it can be skewed by extremely high or low outliers."
    },
    'Median Sync Speed (Blocks/sec sample)': {
        'title': 'Median Sync Speed (Blocks/sec Sampled)',
        'body': "The median is the 50th percentile of the sampled 'Blocks/sec' values. It represents the middle value of the performance data, meaning half of the speed samples were higher and half were lower. The median is often a more robust indicator of central tendency than the mean, as it is less affected by extreme outliers or brief performance spikes/dips."
    },
    'Q3 Sync Speed (Blocks/sec sample)': {
        'title': 'Q3/75th Percentile Sync Speed (Blocks/sec)',
        'body': "The 75th percentile (or third quartile, Q3) is the value below which 75% of the synchronization speed samples fall. This metric highlights the performance level achieved during the faster sync periods. A high Q3 value indicates the node's capability to reach high speeds, but it should be considered alongside the median and mean to understand if this is a frequent or occasional event."
    },
    'Max Sync Speed (Blocks/sec sample)': {
        'title': 'Max Sync Speed (Blocks/sec Sampled)',
        'body': "This metric captures the peak synchronization speed achieved between any two consecutive data points. High maximum values typically occur when the node receives a burst of blocks from a fast peer with low latency, often during periods where the blocks being processed are less computationally intensive."
    },
    'Std Dev of Sync Speed (Blocks/sec sample)': {
        'title': 'Std Dev of Sync Speed (Blocks/sec Sampled)',
        'body': "The standard deviation measures the amount of variation or dispersion of the sampled 'Blocks/sec' values. A low standard deviation indicates that the sync speed was relatively consistent and stable. A high standard deviation suggests significant volatility in performance, with large fluctuations between fast and slow periods. This can be caused by inconsistent network conditions, varying peer quality, or changes in block complexity."
    },
    'Skewness of Sync Speed (Blocks/sec sample)': {
        'title': 'Skewness of Sync Speed (Blocks/sec Sampled)',
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
        'body': "This button filters the currently loaded CSV data, keeping only the header, the first data row (block 0), and every 5000th block thereafter. This action creates a new, smaller CSV file with a `_cleared.csv` suffix in the application's root directory. The filtered data is also used for the current session, which can improve performance and make long-term trends easier to see. This action applies to both the original and comparison files if both are loaded."
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

    def get_stats_dict(df):
        """Helper to calculate stats for a single dataframe, returning both display and raw values."""
        if df.empty or 'Blocks_per_Second' not in df.columns or len(df) < 2:
            na_result = {'display': 'N/A', 'raw': None}
            return {
                'Total Sync in Progress Time': na_result, 'Total Blocks Synced': na_result, 'Overall Average Sync Speed (Blocks/sec)': na_result,
                'Min Sync Speed (Blocks/sec sample)': na_result,
                'Q1 Sync Speed (Blocks/sec sample)': na_result,
                'Mean Sync Speed (Blocks/sec sample)': na_result,
                'Median Sync Speed (Blocks/sec sample)': na_result,
                'Q3 Sync Speed (Blocks/sec sample)': na_result,
                'Max Sync Speed (Blocks/sec sample)': na_result,
                'Std Dev of Sync Speed (Blocks/sec sample)': na_result,
                'Skewness of Sync Speed (Blocks/sec sample)': na_result
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
            'Total Sync in Progress Time': {'display': f"{format_seconds(total_sync_seconds)} ({int(total_sync_seconds)}s)", 'raw': total_sync_seconds},
            'Total Blocks Synced': {'display': f"{total_blocks_synced:,}", 'raw': float(total_blocks_synced)},
            'Overall Average Sync Speed (Blocks/sec)': {'display': f"{overall_avg_bps:.2f}", 'raw': overall_avg_bps},
            'Min Sync Speed (Blocks/sec sample)': {'display': f"{stats.get('min', 0):.2f}", 'raw': stats.get('min', 0.0)},
            'Q1 Sync Speed (Blocks/sec sample)': {'display': f"{stats.get('25%', 0):.2f}", 'raw': stats.get('25%', 0.0)},
            'Mean Sync Speed (Blocks/sec sample)': {'display': f"{stats.get('mean', 0):.2f}", 'raw': stats.get('mean', 0.0)},
            'Median Sync Speed (Blocks/sec sample)': {'display': f"{stats.get('50%', 0):.2f}", 'raw': stats.get('50%', 0.0)},
            'Q3 Sync Speed (Blocks/sec sample)': {'display': f"{stats.get('75%', 0):.2f}", 'raw': stats.get('75%', 0.0)},
            'Max Sync Speed (Blocks/sec sample)': {'display': f"{stats.get('max', 0):.2f}", 'raw': stats.get('max', 0.0)},
            'Std Dev of Sync Speed (Blocks/sec sample)': {'display': f"{stats.get('std', 0):.2f}", 'raw': stats.get('std', 0.0)},
            'Skewness of Sync Speed (Blocks/sec sample)': {'display': f"{skewness:.2f}", 'raw': skewness if pd.notna(skewness) else 0.0}
        }

    stats_original = get_stats_dict(df_original)
    has_comparison = not df_compare.empty

    metric_names = list(stats_original.keys())

    header_cells = [html.Th("Metric"), html.Th(title_original)]
    if has_comparison:
        stats_compare = get_stats_dict(df_compare)
        header_cells.append(html.Th(title_compare))
    
    table_header = [html.Thead(html.Tr(header_cells))]
    
    # Define which metrics are better when higher
    higher_is_better = {
        'Total Sync in Progress Time': False,
        'Total Blocks Synced': True,
        'Overall Average Sync Speed (Blocks/sec)': True,
        'Min Sync Speed (Blocks/sec sample)': True,
        'Q1 Sync Speed (Blocks/sec sample)': True,
        'Mean Sync Speed (Blocks/sec sample)': True,
        'Median Sync Speed (Blocks/sec sample)': True,
        'Q3 Sync Speed (Blocks/sec sample)': True,
        'Max Sync Speed (Blocks/sec sample)': True,
        'Std Dev of Sync Speed (Blocks/sec sample)': False,
        'Skewness of Sync Speed (Blocks/sec sample)': 'closer_to_zero',
    }

    table_body_rows = []
    for metric in metric_names:
        if metric in tooltip_texts:
            info_icon = html.Span([
                " ",
                html.I(className="bi bi-info-circle-fill text-info align-middle", style={'fontSize': '1.1em'}),
            ],
                id={'type': 'info-icon', 'metric': metric},
                style={'cursor': 'pointer'},
                title='Click for more info'
            )
            metric_cell = html.Td([metric, info_icon])
        else:
            metric_cell = html.Td(metric)
            
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
                if metric == 'Total Blocks Synced':
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
    'width': '100%', 'height': '40px', 'lineHeight': '40px',
    'borderWidth': '1px', 'borderStyle': 'dotted', 'borderRadius': '5px',
    'textAlign': 'center', 'margin': '10px 0', 'fontSize': 'small', 'color': 'grey'
}

# --- Run asset setup at startup ---
setup_assets_folder()

# --- App Layout ---
app.layout = html.Div([
    dcc.Store(id='tooltip-store', data=tooltip_texts),
    dcc.Loading(
        id="loading-save",
        type="circle",
        fullscreen=True,
        children=html.Div(id="loading-output-for-save")
    ),
    dcc.Store(id='reports-filepath-store'),
    html.Link(id="theme-stylesheet", rel="stylesheet"),
    dcc.Store(id='theme-store', storage_type='local'),
    dbc.Container([
    dcc.Store(id='tooltip-store', data=tooltip_texts),
    dcc.Store(id='original-data-store', data=initial_original_data),
    dcc.Store(id='compare-data-store'), # No initial data for comparison
    dcc.Store(id='action-feedback-store'), # For modal feedback
    dcc.Store(id='html-content-store'), # For saving HTML content
        dbc.Row([
        dbc.Col(html.H1("Sync Progress Reports", className="mt-3 mb-4"), width="auto", className="me-auto"),
        dbc.Col([
            dbc.Button(html.I(className="bi bi-save"), id="save-button", color="secondary", className="me-3", title="Save Reports as HTML"),
            html.I(className="bi bi-sun-fill", style={'color': 'orange', 'fontSize': '1.2rem'}),
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
                    children=html.Div(['Drag and Drop or ', html.A('Select Comparison sync_progress.csv')]),
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
            dbc.Button("Clear CSV", id="clear-csv-button", color="warning", className="w-100"),
            html.Span([
                " ",
                html.I(className="bi bi-info-circle-fill text-info align-middle", style={'fontSize': '1.1em'}),
            ], # type: ignore
                id={'type': 'info-icon', 'metric': 'Clear CSV'},
                style={'cursor': 'pointer', 'marginLeft': '10px'},
                title='Click for more info'
            )
        ], style={'display': 'flex', 'alignItems': 'center'}), width="auto", className="d-flex align-items-end", id="clear-csv-col", style={'display': 'none' if df_progress.empty else 'flex'}),
    ], className="mt-3"),
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
    Output('original-data-store', 'data', allow_duplicate=True),
    Input('upload-original-progress', 'contents'),
    State('upload-original-progress', 'filename'),
    prevent_initial_call=True
)
def store_original_data(contents, filename):
    if not contents:
        return dash.no_update
    content_type, content_string = contents.split(',')
    decoded = base64.b64decode(content_string)
    try:
        decoded_str = decoded.decode('utf-8')
        lines = decoded_str.splitlines(True) # Keep newlines
        header_row = find_header_row(lines)
        metadata = extract_metadata(lines)
        # Pass only the data part of the file to pandas
        df = pd.read_csv(io.StringIO("".join(lines[header_row:])), sep=';')
        return {'filename': filename, 'data': df.to_json(date_format='iso', orient='split'), 'metadata': metadata}
    except Exception as e:
        print(f"Error parsing original uploaded file: {e}")
        return None

@app.callback(
    Output('compare-data-store', 'data', allow_duplicate=True),
    Input('upload-compare-progress', 'contents'),
    State('upload-compare-progress', 'filename'),
    prevent_initial_call=True
)
def store_compare_data(contents, filename):
    if not contents:
        return dash.no_update
    content_type, content_string = contents.split(',')
    decoded = base64.b64decode(content_string)
    try:
        decoded_str = decoded.decode('utf-8')
        lines = decoded_str.splitlines(True) # Keep newlines
        header_row = find_header_row(lines)
        metadata = extract_metadata(lines)
        # Pass only the data part of the file to pandas
        df = pd.read_csv(io.StringIO("".join(lines[header_row:])), sep=';')
        return {'filename': filename, 'data': df.to_json(date_format='iso', orient='split'), 'metadata': metadata}
    except Exception as e:
        print(f"Error parsing comparison uploaded file: {e}")
        return None

# --- New callback for the clear button ---
@app.callback(
    [Output('original-data-store', 'data', allow_duplicate=True),
     Output('compare-data-store', 'data', allow_duplicate=True),
     Output('action-feedback-store', 'data', allow_duplicate=True)],
    Input('clear-csv-button', 'n_clicks'),
    [State('original-data-store', 'data'),
     State('compare-data-store', 'data')],
    prevent_initial_call=True
)
def clear_csv_data(n_clicks, original_data, compare_data):
    if not n_clicks:
        raise dash.exceptions.PreventUpdate

    feedback_messages = []    
    if original_data and 'data' in original_data:
        df_orig = pd.read_json(io.StringIO(original_data['data']), orient='split')
        rows_before = len(df_orig)
        df_orig_filtered = filter_df_for_clearing(df_orig)
        rows_after = len(df_orig_filtered)
        original_data['data'] = df_orig_filtered.to_json(date_format='iso', orient='split')

        filename = original_data.get('filename', 'Original file')
        # Save to file
        try:
            cleared_dir = os.path.join("measurements", "cleared")
            os.makedirs(cleared_dir, exist_ok=True)
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            base_filename = os.path.basename(filename)
            base, _ = os.path.splitext(base_filename)
            new_filename = f"{base}_{timestamp}_cleared.csv"
            filepath = os.path.join(cleared_dir, new_filename)
            df_orig_filtered.to_csv(filepath, sep=';', index=False)
            feedback_messages.append(
                f"'{filename}' was filtered from {rows_before:,} to {rows_after:,} rows and saved as '{filepath}'."
            )
        except Exception as e:
            feedback_messages.append(
                f"Could not save cleared file for '{filename}'. Error: {e}"
            )

    if compare_data and 'data' in compare_data:
        df_comp = pd.read_json(io.StringIO(compare_data['data']), orient='split')
        rows_before = len(df_comp)
        df_comp_filtered = filter_df_for_clearing(df_comp)
        rows_after = len(df_comp_filtered)
        compare_data['data'] = df_comp_filtered.to_json(date_format='iso', orient='split')
        
        filename = compare_data.get('filename', 'Comparison file')
        # Save to file
        try:
            cleared_dir = os.path.join("measurements", "cleared")
            os.makedirs(cleared_dir, exist_ok=True)
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            base_filename = os.path.basename(filename)
            base, _ = os.path.splitext(base_filename)
            new_filename = f"{base}_{timestamp}_cleared.csv"
            filepath = os.path.join(cleared_dir, new_filename)
            df_comp_filtered.to_csv(filepath, sep=';', index=False)
            feedback_messages.append(
                f"'{filename}' was filtered from {rows_before:,} to {rows_after:,} rows and saved as '{filepath}'."
            )
        except Exception as e:
            feedback_messages.append(
                f"Could not save cleared file for '{filename}'. Error: {e}"
            )

    feedback_body = feedback_messages if feedback_messages else "No data was loaded to clear."
    feedback_data = {'title': 'CSV Data Cleared', 'body': feedback_body}

    return original_data, compare_data, feedback_data

@app.callback(
    Output('upload-original-progress', 'children'),
    Input('original-data-store', 'data')
)
def update_original_upload_text(data):
    """Updates the text of the original upload component based on whether data is loaded."""
    if data and data.get('filename'):
        return html.Div(f"Selected Original file: {data['filename']}")
    return html.Div(['Drag and Drop or ', html.A('Select Original sync_progress.csv')])

@app.callback(
    Output('upload-compare-progress', 'children'),
    Input('compare-data-store', 'data')
)
def update_compare_upload_text(data):
    """Updates the text of the comparison upload component based on whether data is loaded."""
    if data and data.get('filename'):
        return html.Div(f"Selected Comparison file: {data['filename']}")
    return html.Div(['Drag and Drop or ', html.A('Select Comparison sync_progress.csv')])

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
        data_col_names = {
            'Accumulated_sync_in_progress_time[s]': 'Sync Time (s)',
            'SyncTime_Formatted': 'Sync Time (Formatted)',
            'Blocks_per_Second': 'Sync Speed (Blocks/sec)'
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
                html.Th(""), # Spacer for Block Height
                html.Th(f"Original: {original_filename}", colSpan=len(data_col_names), className="text-center", style={'fontWeight': 'normal'}),
                html.Th(f"Comparison: {compare_filename}", colSpan=len(data_col_names), className="text-center", style={'fontWeight': 'normal', **left_border_style})
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
                    'Sync Time (s)': {'higher_is_better': False},
                    'Sync Speed (Blocks/sec)': {'higher_is_better': True}
                }

                # Add original columns' cells
                for display_name in data_col_names.values():
                    val = row.get(f"{display_name}_orig")
                    if pd.isna(val):
                        row_data.append(html.Td(""))
                    elif isinstance(val, float):
                        row_data.append(html.Td(f"{val:.2f}"))
                    else:
                        row_data.append(html.Td(val))

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
                                if display_name == 'Sync Time (s)':
                                    diff_str = f" ({diff:+.2f}s)"
                                else:
                                    diff_str = f" ({diff:+.2f})"
                                cell_content.append(html.Span(diff_str, className=f"small {color_class} fw-bold"))
                    elif display_name == 'Sync Time (Formatted)':
                        orig_s_val = row.get('Sync Time (s)_orig')
                        comp_s_val = row.get('Sync Time (s)_comp')
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
                style={'maxHeight': '500px', 'overflowY': 'auto'}
            )
            table_children.append(scrollable_div)

        elif original_valid:
            # Single table
            col_names = {'Block_height': 'Block Height', **data_col_names}
            df_orig_table = df_original_display[cols_to_show].rename(columns=col_names)

            # --- Title (Non-scrollable) ---
            table_children.append(html.H6(f"Original: {original_filename}"))

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
                style={'maxHeight': '500px', 'overflowY': 'auto'}
            )
            table_children.append(scrollable_div)
        elif compare_valid:
            # Single table for comparison data
            col_names = {'Block_height': 'Block Height', **data_col_names}
            df_comp_table = df_compare_display[cols_to_show].rename(columns=col_names)

            table_children.append(html.H6(f"Comparison: {compare_filename}"))
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
            body_table = dbc.Table([col_group, html.Tbody(body_rows)], striped=True, bordered=True, hover=True, style={'tableLayout': 'fixed', 'width': '100%', 'marginTop': '-1px'})
            scrollable_div = html.Div([header_table, body_table], style={'maxHeight': '500px', 'overflowY': 'auto'})
            table_children.append(scrollable_div)
        else:
            table_children = [html.P("No data to display in table.")]

    return fig, summary_table, dropdown_options, start_block, dropdown_options, end_block, table_children, table_style

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
    # This is the most robust way to check for a real user click.
    # It verifies that the callback was triggered and that the trigger value is a positive integer (not None or 0).
    # This prevents the modal from appearing when its inputs are re-rendered by another callback (e.g., by the slider).
    if not ctx.triggered or not ctx.triggered[0]['value']:
        raise dash.exceptions.PreventUpdate

    triggered_id = ctx.triggered_id
    # A final safety check in case the triggered_id is None
    if not triggered_id:
        raise dash.exceptions.PreventUpdate
    metric_name = triggered_id['metric']
    
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
        if isinstance(body, list):
            body_components = [html.P(p) for p in body]
        else:
            body_components = [html.P(body)]
        
        button_style = {'display': 'inline-block'} if title == 'Reports Saved' else {'display': 'none'}
        return True, title, body_components, button_style

    # In all other cases, don't change the modal state
    return is_open, dash.no_update, dash.no_update, dash.no_update

@app.callback(
    Output('theme-switch', 'value'),
    Output('theme-stylesheet', 'href'),
    Input('theme-store', 'data'),
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

@app.callback(
    [Output('original-metadata-display', 'children'),
     Output('compare-metadata-display', 'children')],
    [Input('original-data-store', 'data'),
     Input('compare-data-store', 'data')]
)
def update_metadata_display(original_data, compare_data):
    def create_metadata_card(data, title_prefix):
        if not data or 'metadata' not in data or not data['metadata']:
            return None

        metadata = data['metadata']
        filename = data.get('filename', 'data file')

        card_header = dbc.CardHeader(f"{title_prefix} System Info: {filename}", className="fw-bold")

        # Define preferred order for display to ensure consistency
        preferred_order = [
            'Signum Version',
            'Hostname',
            'OS Name',
            'OS Version',
            'OS Architecture',
            'Java Version',
            'Available Processors',
            'Max Memory (MB)',
            'Total RAM (MB)',
            'Database Type',
            'Database Version'
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
                    html.Span(value, className="text-end text-muted")
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

        card_body = dbc.ListGroup(list_group_items, flush=True)
        return dbc.Card([card_header, card_body])

    original_card = create_metadata_card(original_data, "Original")
    compare_card = create_metadata_card(compare_data, "Comparison")

    return original_card, compare_card

# --- Callback to apply custom dark theme styles to dropdowns ---

# --- Callbacks for saving HTML report ---
app.clientside_callback(
    """
    async function(n_clicks, slider_value_index, figure) {
        if (!n_clicks) {
            // This is the initial call or a callback update where the button wasn't clicked.
            return [window.dash_clientside.no_update, window.dash_clientside.no_update];
        }

        if (!figure) {
            const noFigureReturn = [window.dash_clientside.no_update, window.dash_clientside.no_update];
            return noFigureReturn;
        }

        try {
            const mainContainer = document.getElementById('main-container');
            if (!mainContainer) {
                return ['CLIENTSIDE_ERROR: main-container element not found.', null];
            }
            // Clone the container to avoid modifying the live DOM
            const clone = mainContainer.cloneNode(true);

            const isDarkTheme = document.documentElement.getAttribute('data-bs-theme') === 'dark';

            // --- Replace Dropdowns with Static Text ---
            const dropdownIds = ['start-block-dropdown', 'end-block-dropdown'];
            dropdownIds.forEach(id => {
                const originalDropdownValue = document.querySelector(`#${id} .Select-value-label, #${id} .Select__single-value`);
                const clonedDropdown = clone.querySelector(`#${id}`);
                if (clonedDropdown && clonedDropdown.parentNode) {
                    const valueText = originalDropdownValue ? originalDropdownValue.textContent : 'N/A';
                    const staticEl = document.createElement('div');
                    staticEl.textContent = valueText;
                    // Use CSS variables to match the dropdown style from DROPDOWN_CSS
                    staticEl.style.cssText = `
                        margin-top: 0px;
                        padding: 6px 12px;
                        border-radius: 4px;
                        font-size: 0.9rem;
                        background-color: var(--bs-body-bg);
                        border: 1px solid var(--bs-border-color);
                        color: var(--bs-body-color);
                    `;
                    clonedDropdown.parentNode.replaceChild(staticEl, clonedDropdown);
                }
            });

            // --- Replace Slider with Static Text ---
            const ma_windows = [10, 100, 200, 300, 400, 500];
            const window_size = ma_windows[slider_value_index];
            const sliderContainer = clone.querySelector('#ma-slider-container');
            if (sliderContainer) {
                const staticEl = document.createElement('div');
                staticEl.textContent = `Moving Average Window: ${window_size}`;
                staticEl.className = 'mt-3'; // Add some margin
                staticEl.style.fontWeight = 'bold';
                sliderContainer.parentNode.replaceChild(staticEl, sliderContainer);
            }

            // --- Convert Plotly graph to a static image ---
            const graphDiv = clone.querySelector('#progress-graph');
            const originalGraphDiv = document.getElementById('progress-graph');

            if (graphDiv && originalGraphDiv && window.Plotly) {
                const tempDiv = document.createElement('div');
                // Position it off-screen
                tempDiv.style.position = 'absolute';
                tempDiv.style.left = '-9999px';
                tempDiv.style.width = originalGraphDiv.offsetWidth + 'px';
                tempDiv.style.height = originalGraphDiv.offsetHeight + 'px';
                document.body.appendChild(tempDiv);

                try {
                    // Use the figure data passed directly to the callback
                    const data = JSON.parse(JSON.stringify(figure.data));
                    const layout = JSON.parse(JSON.stringify(figure.layout));

                    if (isDarkTheme) {
                        layout.paper_bgcolor = '#222529'; // Darkly theme background
                        layout.plot_bgcolor = '#222529';
                    }

                    // --- Increase font sizes for better readability in the saved image ---
                    const fontSizeIncrease = 6; // Increase font size by 6 points
                    if (layout.title) {
                        layout.title.font = layout.title.font || {};
                        layout.title.font.size = (layout.title.font.size || 16) + fontSizeIncrease;
                    }
                    if (layout.xaxis) {
                        layout.xaxis.title = layout.xaxis.title || {};
                        layout.xaxis.title.font = layout.xaxis.title.font || {};
                        layout.xaxis.title.font.size = (layout.xaxis.title.font.size || 12) + fontSizeIncrease + 2; // Axis titles a bit larger
                        layout.xaxis.tickfont = layout.xaxis.tickfont || {};
                        layout.xaxis.tickfont.size = (layout.xaxis.tickfont.size || 12) + fontSizeIncrease;
                    }
                    if (layout.yaxis) {
                        layout.yaxis.title = layout.yaxis.title || {};
                        layout.yaxis.title.font = layout.yaxis.title.font || {};
                        layout.yaxis.title.font.size = (layout.yaxis.title.font.size || 12) + fontSizeIncrease + 2;
                        layout.yaxis.tickfont = layout.yaxis.tickfont || {};
                        layout.yaxis.tickfont.size = (layout.yaxis.tickfont.size || 12) + fontSizeIncrease;
                    }
                    if (layout.yaxis2) {
                        layout.yaxis2.title = layout.yaxis2.title || {};
                        layout.yaxis2.title.font = layout.yaxis2.title.font || {};
                        layout.yaxis2.title.font.size = (layout.yaxis2.title.font.size || 12) + fontSizeIncrease + 2;
                        layout.yaxis2.tickfont = layout.yaxis2.tickfont || {};
                        layout.yaxis2.tickfont.size = (layout.yaxis2.tickfont.size || 12) + fontSizeIncrease;
                    }
                    if (layout.legend) {
                        layout.legend.font = layout.legend.font || {};
                        layout.legend.font.size = (layout.legend.font.size || 10) + fontSizeIncrease + 2;
                    }

                    // Create a temporary plot
                    await window.Plotly.newPlot(tempDiv, data, layout);

                    // Render at a slightly larger base size for better readability of text and lines
                    const renderWidth = originalGraphDiv.offsetWidth * 1.4;
                    const renderHeight = originalGraphDiv.offsetHeight * 1.4;

                    const dataUrl = await window.Plotly.toImage(tempDiv, {
                        format: 'png',
                        height: renderHeight,
                        width: renderWidth,
                        scale: 2 // 1.4x base size with 2x scale gives a sharp 2.8x total resolution
                    });

                    const img = document.createElement('img');
                    img.src = dataUrl;
                    // Display the image even larger than its container for better readability.
                    img.style.width = '150%';
                    img.style.position = 'relative';
                    img.style.left = '-25%'; // Center the oversized image
                    img.style.height = 'auto';
                    graphDiv.parentNode.replaceChild(img, graphDiv);
                } catch (e) {
                    console.error('Plotly.toImage failed:', e);
                    const p = document.createElement('p');
                    p.innerText = '[Error converting chart to image]';
                    p.style.color = 'red';
                    graphDiv.parentNode.replaceChild(p, graphDiv);
                } finally {
                    // Always remove the temporary div
                    document.body.removeChild(tempDiv);
                }
            } else {
                if (graphDiv && graphDiv.parentNode) {
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
                            .catch(() => ''); // Silently fail on fetch errors
                    } else if (sheet.cssRules) {
                        let rules = '';
                        for (let i = 0; i < sheet.cssRules.length; i++) {
                            rules += sheet.cssRules[i].cssText;
                        }
                        return Promise.resolve(rules);
                    }
                } catch (e) {
                    // Silently fail on security errors for cross-origin sheets
                }
                return Promise.resolve(''); // Return empty promise for unreadable sheets
            });

            const cssContents = await Promise.all(cssPromises);
            cssText = cssContents.join('\\n'); // Use '\\n' for JS newlines

            // --- Escape backticks and other problematic characters ---
            // Only backticks need escaping inside a template literal.
            const cleanCssText = cssText.replace(/`/g, '\\`');
            const cleanOuterHtml = clone.outerHTML.replace(/`/g, '\\`');

            // --- Remove unwanted elements from the cloned report ---
            const elementsToRemove = clone.querySelectorAll('#save-button, #theme-switch, .bi-sun-fill, .bi-moon-stars-fill, #clear-csv-button, #reset-view-button, #show-data-table-switch-container, #original-upload-container, #compare-upload-container, span[id*="info-icon"]');
            elementsToRemove.forEach(el => el.parentNode.removeChild(el));

            // Construct the full HTML document
            const fullHtml = `
                <!DOCTYPE html>
                <html lang="en">
                    <head>
                        <meta charset="utf-8">
                        <title>Sync Progress Reports</title>
                        <style>
                            ${cleanCssText}
                            /* Custom styles for saved report */
                            body { 
                                font-family: sans-serif; 
                            }
                            .container-fluid { 
                                max-width: 1200px !important; 
                                margin: 0 auto !important; 
                                padding: 20px !important; 
                                float: none !important;
                            }
                            /* Hide interactive elements in case removal fails. */
                            #save-button, #theme-switch, .bi-sun-fill, .bi-moon-stars-fill, #ma-slider-container, #clear-csv-button, #reset-view-button, #show-data-table-switch-container, #original-upload-container, #compare-upload-container, span[id*="info-icon"] {
                                display: none !important;
                            }
                            /* Ensure all text is selectable */
                            body, body * {
                                -webkit-user-select: text !important;
                                -moz-user-select: text !important;
                                -ms-user-select: text !important;
                                user-select: text !important;
                            }
                        </style>
                    </head>
                    <body>
                        ${cleanOuterHtml}
                    </body>
                </html>
            `;

            // Return content for store and a dummy value to deactivate the spinner
            return [fullHtml, null];
        } catch (e) {
            alert('Caught an error in callback: ' + e.message);
            const error_content = 'CLIENTSIDE_ERROR: ' + e.message + '\\n' + e.stack;
            // Return error to store and dummy value for spinner
            return [error_content, null];
        }
    }
    """,
    [Output('html-content-store', 'data'),
     Output('loading-output-for-save', 'children')],
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
    # Generate a dynamic filename with timestamp
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"sync_progress_reports_{timestamp}.html"
    try:
        reports_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "reports")
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