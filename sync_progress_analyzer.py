import pandas as pd
import dash
from dash import dcc, html
from dash.dependencies import Input, Output, State
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import base64
import io
from datetime import timedelta
from plotly.subplots import make_subplots

# --- Data Loading ---
# Load sync progress data
try:
    df_progress = pd.read_csv("sync_progress.csv", sep=";")
except FileNotFoundError:
    df_progress = pd.DataFrame(columns=['Accumulated_sync_in_progress_time[s]', 'Block_height'])
    print("Warning: sync_progress.csv not found. Progress graph will be empty.")

# --- Helper Function ---
def format_seconds(seconds):
    """Formats seconds into a human-readable D-H-M-S string."""
    if pd.isna(seconds):
        return "N/A"
    return str(timedelta(seconds=int(seconds)))

# --- Tooltip Content ---
tooltip_texts = {
    'Original sync_progress.csv': {
        'title': 'Original sync_progress.csv',
        'body': "This is the primary data file for analysis. It should be a CSV file containing synchronization progress data, typically named `sync_progress.csv`. The data from this file will be used to generate the main graphs and metrics. You can either drag and drop the file here or click to select it from your computer."
    },
    'Comparison sync_progress.csv': {
        'title': 'Comparison sync_progress.csv',
        'body': "This is an optional secondary data file used for comparison. If you provide a second `sync_progress.csv` file here, the application will display its data alongside the original data, allowing for a side-by-side performance comparison. This is useful for evaluating the impact of changes in node configuration, hardware, or network conditions."
    },
    'Total Sync in Progress Time': {
        'title': 'Total Sync in Progress Time',
        'body': "This metric represents the total duration the node has spent in an active synchronization state. The timer for this metric is active only when the node's block height is significantly behind the network's current height (typically more than 10 blocks). It provides a precise measure of the time required to catch up with the blockchain, excluding periods when the node is already fully synchronized or idle. This is a key performance indicator for evaluating the efficiency of the sync process under load."
    },
    'Total Blocks Synced': {
        'title': 'Total Blocks Synced',
        'body': "This value indicates the total number of blockchain blocks that have been downloaded, verified, and applied by the node during the measurement period. It is calculated as the difference between the final and initial block height. This metric, in conjunction with the sync time, is fundamental for calculating the overall synchronization speed."
    },
    'Overall Average Blocks/sec': {
        'title': 'Overall Average Blocks/sec',
        'body': "This is a high-level performance metric calculated by dividing the 'Total Blocks Synced' by the 'Total Sync in Progress Time' in seconds. It provides a single, averaged value representing the node's throughput over the entire synchronization process. While useful for a quick comparison, it can mask variations in performance that occur at different stages of syncing."
    },
    'Min Blocks/sec (sample)': {
        'title': 'Min Blocks/sec (Sampled)',
        'body': "This metric shows the minimum synchronization speed observed between any two consecutive data points in the sample. A very low or zero value can indicate periods of network latency, slow peer response, or high computational load on the node (e.g., during verification of blocks with many complex transactions), causing a temporary stall in progress."
    },
    '25th Percentile Blocks/sec (Q1)': {
        'title': '25th Percentile (Q1) Blocks/sec',
        'body': "The 25th percentile (or first quartile, Q1) is the value below which 25% of the synchronization speed samples fall. It indicates the performance level that the node meets or exceeds 75% of the time. A higher Q1 value suggests that the node consistently maintains a good minimum performance level, with fewer periods of very low speed."
    },
    'Mean Blocks/sec (sample)': {
        'title': 'Mean Blocks/sec (Sampled)',
        'body': "This represents the statistical average (mean) of the 'Blocks/sec' values calculated for each interval between data points. Unlike the 'Overall Average', this metric is the average of individual speed samples, giving a more granular view of the typical performance, but it can be skewed by extremely high or low outliers."
    },
    'Median Blocks/sec (sample)': {
        'title': 'Median Blocks/sec (Sampled)',
        'body': "The median is the 50th percentile of the sampled 'Blocks/sec' values. It represents the middle value of the performance data, meaning half of the speed samples were higher and half were lower. The median is often a more robust indicator of central tendency than the mean, as it is less affected by extreme outliers or brief performance spikes/dips."
    },
    '75th Percentile Blocks/sec (Q3)': {
        'title': '75th Percentile (Q3) Blocks/sec',
        'body': "The 75th percentile (or third quartile, Q3) is the value below which 75% of the synchronization speed samples fall. This metric highlights the performance level achieved during the faster sync periods. A high Q3 value indicates the node's capability to reach high speeds, but it should be considered alongside the median and mean to understand if this is a frequent or occasional event."
    },
    'Max Blocks/sec (sample)': {
        'title': 'Max Blocks/sec (Sampled)',
        'body': "This metric captures the peak synchronization speed achieved between any two consecutive data points. High maximum values typically occur when the node receives a burst of blocks from a fast peer with low latency, often during periods where the blocks being processed are less computationally intensive."
    },
    'Std Dev Blocks/sec (sample)': {
        'title': 'Std Dev Blocks/sec (Sampled)',
        'body': "The standard deviation measures the amount of variation or dispersion of the sampled 'Blocks/sec' values. A low standard deviation indicates that the sync speed was relatively consistent and stable. A high standard deviation suggests significant volatility in performance, with large fluctuations between fast and slow periods. This can be caused by inconsistent network conditions, varying peer quality, or changes in block complexity."
    },
    'Skewness Blocks/sec (sample)': {
        'title': 'Skewness of Blocks/sec (Sampled)',
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
        'body': "This button filters the currently loaded CSV data, keeping only the header, the first data row (block 0), and every 5000th block thereafter. This significantly reduces the number of data points, which can improve performance and make long-term trends easier to see. The filtered data remains in memory for the current session but does not modify the original uploaded file. This action applies to both the original and comparison files if both are loaded."
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
    'Blocks/sec': {
        'title': 'Blocks per Second (Instantaneous)',
        'body': "The instantaneous synchronization speed, calculated as the number of blocks processed since the last data point, divided by the time elapsed during that interval. This metric shows the node's performance at a specific moment and can fluctuate based on network conditions and block complexity."
    }
}

# --- Prepare initial data for the store if df_progress is loaded ---
initial_original_data = None
if not df_progress.empty:
    initial_original_data = {
        'filename': 'sync_progress.csv',
        'data': df_progress.to_json(date_format='iso', orient='split')
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
                'Total Sync in Progress Time': na_result, 'Total Blocks Synced': na_result,
                'Overall Average Blocks/sec': na_result,
                'Min Blocks/sec (sample)': na_result,
                '25th Percentile Blocks/sec (Q1)': na_result,
                'Mean Blocks/sec (sample)': na_result,
                'Median Blocks/sec (sample)': na_result,
                '75th Percentile Blocks/sec (Q3)': na_result,
                'Max Blocks/sec (sample)': na_result,
                'Std Dev Blocks/sec (sample)': na_result,
                'Skewness Blocks/sec (sample)': na_result
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
            'Overall Average Blocks/sec': {'display': f"{overall_avg_bps:.2f}", 'raw': overall_avg_bps},
            'Min Blocks/sec (sample)': {'display': f"{stats.get('min', 0):.2f}", 'raw': stats.get('min', 0.0)},
            '25th Percentile Blocks/sec (Q1)': {'display': f"{stats.get('25%', 0):.2f}", 'raw': stats.get('25%', 0.0)},
            'Mean Blocks/sec (sample)': {'display': f"{stats.get('mean', 0):.2f}", 'raw': stats.get('mean', 0.0)},
            'Median Blocks/sec (sample)': {'display': f"{stats.get('50%', 0):.2f}", 'raw': stats.get('50%', 0.0)},
            '75th Percentile Blocks/sec (Q3)': {'display': f"{stats.get('75%', 0):.2f}", 'raw': stats.get('75%', 0.0)},
            'Max Blocks/sec (sample)': {'display': f"{stats.get('max', 0):.2f}", 'raw': stats.get('max', 0.0)},
            'Std Dev Blocks/sec (sample)': {'display': f"{stats.get('std', 0):.2f}", 'raw': stats.get('std', 0.0)},
            'Skewness Blocks/sec (sample)': {'display': f"{skewness:.2f}", 'raw': skewness if pd.notna(skewness) else 0.0}
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
        'Overall Average Blocks/sec': True,
        'Min Blocks/sec (sample)': True,
        '25th Percentile Blocks/sec (Q1)': True,
        'Mean Blocks/sec (sample)': True,
        'Median Blocks/sec (sample)': True,
        '75th Percentile Blocks/sec (Q3)': True,
        'Max Blocks/sec (sample)': True,
        'Std Dev Blocks/sec (sample)': False,
        'Skewness Blocks/sec (sample)': 'closer_to_zero',
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

def process_progress_df(df):
    """Adds calculated columns to the progress dataframe."""
    if df.empty:
        return df
    df['SyncTime_Formatted'] = df['Accumulated_sync_in_progress_time[s]'].apply(format_seconds)
    delta_blocks = df['Block_height'].diff()
    delta_time = df['Accumulated_sync_in_progress_time[s]'].diff()
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

# --- App Layout ---
app.layout = html.Div([
    dcc.Store(id='theme-store', storage_type='local'),
    html.Link(id="theme-stylesheet", rel="stylesheet"),
    dbc.Container([
    dcc.Store(id='original-data-store', data=initial_original_data),
    dcc.Store(id='compare-data-store'), # No initial data for comparison
    dcc.Store(id='action-feedback-store'), # For modal feedback
        dbc.Row([
        dbc.Col(html.H1("Sync Progress Report", className="mt-3 mb-4"), width="auto", className="me-auto"),
        dbc.Col([
            html.I(className="bi bi-sun-fill", style={'color': 'orange', 'fontSize': '1.2rem'}),
            dbc.Switch(id="theme-switch", value=True, className="d-inline-block mx-2"),
            html.I(className="bi bi-moon-stars-fill", style={'color': 'royalblue', 'fontSize': '1.2rem'}),
        ], width="auto", className="d-flex align-items-center mt-3")
    ], align="center"),

    # --- Sync Progress Section ---
    html.H3("Sync Progress"),
    dbc.Row([
        dbc.Col(html.Div([
            html.Div(dcc.Upload(
                id='upload-original-progress',
                children=html.Div(['Drag and Drop or ', html.A('Select Original sync_progress.csv')]),
                style=upload_style,
                multiple=False
            ), style={'flexGrow': 1}),
            html.Span([
                " ",
                html.I(className="bi bi-info-circle-fill text-info align-middle", style={'fontSize': '1.1em'}),
            ],
                id={'type': 'info-icon', 'metric': 'Original sync_progress.csv'},
                style={'cursor': 'pointer', 'marginLeft': '10px'},
                title='Click for more info'
            )
        ], style={'display': 'flex', 'alignItems': 'center'})),
        dbc.Col(html.Div([
            html.Div(dcc.Upload(
                id='upload-compare-progress',
                children=html.Div(['Drag and Drop or ', html.A('Select Comparison sync_progress.csv')]),
                style=upload_style,
                multiple=False
            ), style={'flexGrow': 1}),
            html.Span([
                " ",
                html.I(className="bi bi-info-circle-fill text-info align-middle", style={'fontSize': '1.1em'}),
            ],
                id={'type': 'info-icon', 'metric': 'Comparison sync_progress.csv'},
                style={'cursor': 'pointer', 'marginLeft': '10px'},
                title='Click for more info'
            )
        ], style={'display': 'flex', 'alignItems': 'center'})),
    ]),
    dbc.Row([
        dbc.Col([
            html.Label("Start Block Height:"),
            dcc.Dropdown(id='start-block-dropdown', clearable=False, placeholder="Select start block")
        ], width=3),
        dbc.Col([
            html.Label("End Block Height:"),
            dcc.Dropdown(id='end-block-dropdown', clearable=False, placeholder="Select end block")
        ], width=3),
        dbc.Col([
            html.Div([
                dbc.Button("Reset View", id="reset-view-button", color="secondary", className="w-100"),
                html.Span([
                    " ",
                    html.I(className="bi bi-info-circle-fill text-info align-middle", style={'fontSize': '1.1em'}),
                ],
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
            ],
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
        ],
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
        ),
    ], style={'display': 'flex', 'alignItems': 'center', 'marginTop': '20px'}),
    html.Div(id="total-time-display-container"),

    html.Hr(),
    html.H3("Raw Data View", className="mt-4"),
    dbc.Switch(
        id='show-data-table-switch',
        label="Show Raw Data Table",
        value=False,
    ),
    html.Div(id='data-table-container'),

    # Add Modal to layout
    dbc.Modal(
        [
            dbc.ModalHeader(dbc.ModalTitle(id="tooltip-modal-title")),
            dbc.ModalBody(id="tooltip-modal-body"),
        ],
        id="tooltip-modal",
        is_open=False,
        size="lg", # Larger modal for more text
    ),
    dbc.Modal(
        [
            dbc.ModalHeader(dbc.ModalTitle(id="action-feedback-modal-title")),
            dbc.ModalBody(id="action-feedback-modal-body"),
            dbc.ModalFooter(
                dbc.Button(
                    "Close", id="action-feedback-modal-close", className="ms-auto", n_clicks=0
                )
            ),
        ],
        id="action-feedback-modal",
        is_open=False,
    )], fluid=True, id="main-container")
])


# --- New Callbacks for handling uploads and storing data ---
@app.callback(
    Output('original-data-store', 'data'),
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
        df = pd.read_csv(io.StringIO(decoded.decode('utf-8')), sep=';')
        return {'filename': filename, 'data': df.to_json(date_format='iso', orient='split')}
    except Exception as e:
        print(f"Error parsing original uploaded file: {e}")
        return None

@app.callback(
    Output('compare-data-store', 'data'),
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
        df = pd.read_csv(io.StringIO(decoded.decode('utf-8')), sep=';')
        return {'filename': filename, 'data': df.to_json(date_format='iso', orient='split')}
    except Exception as e:
        print(f"Error parsing comparison uploaded file: {e}")
        return None

# --- New callback for the clear button ---
@app.callback(
    [Output('original-data-store', 'data', allow_duplicate=True),
     Output('compare-data-store', 'data', allow_duplicate=True),
     Output('action-feedback-store', 'data')],
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
        feedback_messages.append(
            f"'{filename}' was filtered from {rows_before:,} to {rows_after:,} rows."
        )

    if compare_data and 'data' in compare_data:
        df_comp = pd.read_json(io.StringIO(compare_data['data']), orient='split')
        rows_before = len(df_comp)
        df_comp_filtered = filter_df_for_clearing(df_comp)
        rows_after = len(df_comp_filtered)
        compare_data['data'] = df_comp_filtered.to_json(date_format='iso', orient='split')

        filename = compare_data.get('filename', 'Comparison file')
        feedback_messages.append(
            f"'{filename}' was filtered from {rows_before:,} to {rows_after:,} rows."
        )
    
    feedback_body = feedback_messages if feedback_messages else "No data was loaded to clear."
    feedback_data = {'title': 'CSV Data Cleared', 'body': feedback_body}

    return original_data, compare_data, feedback_data

# --- Callbacks to Update Upload Component Text ---
@app.callback(
    Output('upload-original-progress', 'children'),
    [Input('upload-original-progress', 'filename')]
)
def update_original_upload_text(filename):
    if filename:
        return html.Div(f'Selected Original file: {filename}')
    return html.Div(['Drag and Drop or ', html.A('Select Original sync_progress.csv')])


@app.callback(
    Output('upload-compare-progress', 'children'),
    [Input('upload-compare-progress', 'filename')]
)
def update_compare_upload_text(filename):
    if filename:
        return html.Div(f'Selected Comparison file: {filename}')
    return html.Div(['Drag and Drop or ', html.A('Select Comparison sync_progress.csv')])

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
     Output('action-feedback-store', 'data', allow_duplicate=True)],
    [Input("ma-window-slider-progress", "value"),
     Input('original-data-store', 'data'),
     Input('compare-data-store', 'data'),
     Input('start-block-dropdown', 'value'),
     Input('end-block-dropdown', 'value'),
     Input('reset-view-button', 'n_clicks'),
     Input('show-data-table-switch', 'value'),
     Input('theme-store', 'data')],
    prevent_initial_call=True
)
def update_progress_graph_and_time(window_index, original_data, compare_data,
                                   start_block_val, end_block_val, reset_clicks,
                                   show_data_table, theme):
    window = ma_windows[window_index]
    ctx = dash.callback_context

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
        empty_fig = go.Figure()
        empty_fig.update_layout(title_text='Upload a sync_progress.csv file to begin')
        no_data_msg = html.P("No sync progress data to display.", className="text-left")
        return empty_fig, no_data_msg, [], None, [], None, [], {'display': 'none'}

    # --- Process full dataframes first to get Blocks_per_Second ---
    if not df_progress_local.empty:
        df_progress_local = process_progress_df(df_progress_local)
    if not df_compare.empty:
        df_compare = process_progress_df(df_compare)

    # --- Determine block range ---
    block_height_series = []
    if not df_progress_local.empty:
        block_height_series.append(df_progress_local['Block_height'])
    if not df_compare.empty:
        block_height_series.append(df_compare['Block_height'])

    all_block_heights = pd.concat(block_height_series).dropna().unique()
    all_block_heights.sort()

    dropdown_options = [{'label': f"{int(h):,}", 'value': h} for h in all_block_heights]
    min_block = all_block_heights[0] if len(all_block_heights) > 0 else 0
    max_block = all_block_heights[-1] if len(all_block_heights) > 0 else 0

    triggered_id_str = ctx.triggered[0]['prop_id'].split('.')[0] if ctx.triggered else ''
    is_upload_or_clear = triggered_id_str in ['original-data-store', 'compare-data-store']
    is_reset = triggered_id_str == 'reset-view-button'

    feedback_data = dash.no_update
    if is_reset:
        feedback_data = {
            'title': 'View Reset',
            'body': 'The block range filter has been reset to show all available data.'
        }

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
                name='Blocks/sec (Original)',
                line=dict(color='darkorange', dash='dot', width=1),
                hovertemplate='<b>Blocks/sec</b>: %{y:.2f}<extra></extra>'
            ),
            secondary_y=True,
        )
        fig.add_trace(
            go.Scatter(
                x=df_original_display['Accumulated_sync_in_progress_time[s]'],
                y=df_original_display['BPS_ma'],
                name='Blocks/sec (MA) (Original)',
                line=dict(color='orange', dash='solid'),
                hovertemplate='<b>Blocks/sec (MA)</b>: %{y:.2f}<extra></extra>'
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
                    name='Blocks/sec (Comparison)',
                    line=dict(color='fuchsia', dash='dot', width=1),
                    hovertemplate='<b>Blocks/sec</b>: %{y:.2f}<extra></extra>'
                ),
                secondary_y=True,
            )
        fig.add_trace(
                go.Scatter(
                    x=df_compare_display['Accumulated_sync_in_progress_time[s]'],
                    y=df_compare_display['BPS_ma'],
                    name='Blocks/sec (MA) (Comparison)',
                    line=dict(color='magenta', dash='solid'),
                    hovertemplate='<b>Blocks/sec (MA)</b>: %{y:.2f}<extra></extra>'
                ),
                secondary_y=True,
            )

    # Update layout and axes
    graph_template = 'plotly_dark' if theme != 'light' else 'plotly'
    fig.update_layout(
        title_text=f'Block Height vs. Sync Time (MA Window: {window})',
        height=600,
        hovermode='x unified', hoverlabel=dict(bgcolor="rgba(255, 255, 255, 0.8)", font=dict(color='black')),
        xaxis_showspikes=True, xaxis_spikemode='across', xaxis_spikedash='dot',
        yaxis_showspikes=True, yaxis_spikedash='dot', yaxis2_showspikes=True, yaxis2_spikedash='dot',
        legend=dict(orientation="v",
                    yanchor="top",
                    y=-0.2,
                    xanchor="left",
                    x=0,
                    itemclick='toggle',
                    itemdoubleclick='toggleothers')
    )
    fig.update_layout(template=graph_template)
    fig.update_layout(
        margin=dict(l=80, r=0, t=80, b=80)
    )
    fig.update_yaxes(title_text="<b>Block Height</b>", secondary_y=False)
    fig.update_yaxes(title_text="<b>Blocks / Second</b>", secondary_y=True)
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
        # The container itself is not scrollable. Scrolling is handled by an inner div.
        table_style = {'display': 'block'}

        # Rename columns for better readability
        data_col_names = {
            'Accumulated_sync_in_progress_time[s]': 'Sync Time (s)',
            'SyncTime_Formatted': 'Sync Time (Formatted)',
            'Blocks_per_Second': 'Blocks/sec'
        }

        # Columns to select from the original dataframes
        cols_to_show = ['Block_height'] + list(data_col_names.keys())

        has_comparison = not df_compare_display.empty
        
        if has_comparison:
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
            orig_cols = [f"{col}_orig" for col in data_col_names.values()]
            comp_cols = [f"{col}_comp" for col in data_col_names.values()]

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
                'position': 'sticky', 'top': 0, 'zIndex': 2,
                'backgroundColor': 'var(--bs-body-bg, white)'
            }
            header_table = dbc.Table([col_group] + header_table_children, bordered=True, className="mb-0", style=header_table_style)
            # --- Build Body Table ---
            body_rows = []
            for _, row in df_merged.iterrows():
                row_data = [html.Td(f"{int(row['Block Height']):,}" if pd.notna(row['Block Height']) else "")]
                
                numeric_metrics_info = {
                    'Sync Time (s)': {'higher_is_better': False},
                    'Blocks/sec': {'higher_is_better': True}
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

        elif not df_original_display.empty:
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
                'position': 'sticky', 'top': 0, 'zIndex': 2,
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
        else:
            table_children = [html.P("No data to display in table.")]

    return fig, summary_table, dropdown_options, start_block, dropdown_options, end_block, table_children, table_style, dash.no_update
    return fig, summary_table, dropdown_options, start_block, dropdown_options, end_block, table_children, table_style, feedback_data

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
     Output("action-feedback-modal-body", "children")],
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
        return False, dash.no_update, dash.no_update

    # If new feedback data arrived, open the modal and set content
    if triggered_id == "action-feedback-store" and feedback_data:
        title = feedback_data.get('title', 'Notification')
        body = feedback_data.get('body', 'An action was completed.')
        
        # Handle body if it's a list of strings or a single string
        if isinstance(body, list):
            body_components = [html.P(p) for p in body]
        else:
            body_components = [html.P(body)]
            
        return True, title, body_components

    # In all other cases, don't change the modal state
    return is_open, dash.no_update, dash.no_update

@app.callback(
    Output('theme-switch', 'value'),
    Output('theme-stylesheet', 'href'),
    Input('theme-store', 'data'),
)
def load_initial_theme(stored_theme):
    # Default to dark theme if nothing is stored
    if stored_theme == 'light':
        return False, dbc.themes.BOOTSTRAP
    return True, dbc.themes.VAPOR

@app.callback(
    Output('theme-store', 'data', allow_duplicate=True),
    Output('theme-stylesheet', 'href', allow_duplicate=True),
    Input('theme-switch', 'value'),
    prevent_initial_call=True
)
def switch_theme(is_dark):
    if is_dark:
        return 'dark', dbc.themes.VAPOR
    return 'light', dbc.themes.BOOTSTRAP

if __name__ == "__main__":
    app.run(debug=True, port=8050)