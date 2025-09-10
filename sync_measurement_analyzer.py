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
try:
    df_measurement = pd.read_csv("sync_measurement.csv", sep=";", low_memory=False)
except FileNotFoundError:
    df_measurement = pd.DataFrame() # empty dataframe
    print("Error: sync_measurement.csv not found. Cannot generate performance reports.")

# --- Helper Function ---
def format_seconds(seconds):
    """Formats seconds into a human-readable D-H-M-S string."""
    if pd.isna(seconds):
        return "N/A"
    return str(timedelta(seconds=int(seconds)))

def process_measurement_df(df):
    """Adds calculated columns to the measurement dataframe."""
    if df.empty:
        return df
    df["SyncInProgressTime_s"] = df["Accumulated_sync_in_progress_time[ms]"] / 1000
    df['SyncTime_Formatted'] = df['SyncInProgressTime_s'].apply(format_seconds)
    df['Transactions_per_Second'] = (df['Transaction_count'] / (df['Push_block_time[ms]'] / 1000)).replace([pd.NA, float('inf')], 0)
    df['Blocks_per_Second'] = (1000 / df['Push_block_time[ms]']).replace([pd.NA, float('inf')], 0)
    df['Time_per_Transaction_ms'] = (df['Push_block_time[ms]'] / df['Transaction_count']).replace([pd.NA, float('inf')], 0)
    return df

# --- Metrics Definition ---
time_metrics = [
    "Push_block_time[ms]",
    "Validation_time[ms]",
    "Tx_loop_time[ms]",
    "Housekeeping_time[ms]",
    "Tx_apply_time[ms]",
    "AT_time[ms]",
    "Subscription_time[ms]",
    "Block_apply_time[ms]",
    "Commit_time[ms]"
]
other_metrics = ["Transaction_count"]
all_metrics = time_metrics + other_metrics

# --- Statistical Summary ---
if not df_measurement.empty:
    summary = df_measurement[all_metrics].describe().T[["min", "mean", "50%", "max", "std"]]
    summary.rename(columns={"50%": "median", "std": "stddev"}, inplace=True)
else:
    summary = pd.DataFrame() # empty summary if no data

# --- Moving Average Window Values ---
ma_windows = [10, 100, 200, 300, 400, 500]
ma_marks = {i: str(v) for i, v in enumerate(ma_windows)}

# --- Dash App Initialization ---
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

# --- Common Styles ---
upload_style = {
    'width': '100%', 'height': '40px', 'lineHeight': '40px',
    'borderWidth': '1px', 'borderStyle': 'dotted', 'borderRadius': '5px',
    'textAlign': 'center', 'margin': '10px 0', 'fontSize': 'small', 'color': 'grey'
}

# --- App Layout ---
app.layout = dbc.Container([
    html.H1("Sync Performance Report", className="mt-3 mb-4"),

    dcc.Upload(
        id='upload-measurement',
        children=html.Div(['Drag and Drop or ', html.A('Select sync_measurement.csv')]),
        style=upload_style,
        multiple=False
    ),
    html.Hr(),

    # --- Throughput Analysis Section ---
    html.H3("Throughput Analysis"),
    dcc.Checklist(
        id='show-throughput-checkbox',
        options=[{'label': 'Show Throughput Graph', 'value': 'show'}],
        value=['show'], # Default to checked
        style={'textAlign': 'left', 'padding': '10px'}
    ),
    dcc.Graph(id="throughput-graph"),
    html.Hr(),

    # --- Performance Metrics Section ---
    html.H3("Performance Metrics"),
    html.H5("Statistical Summary", className="mt-4"),
    html.Div(id="summary-table-container", children=[
        dbc.Table.from_dataframe(summary.round(2), striped=True, bordered=True, hover=True) if not summary.empty else html.P("No measurement data to display.")
    ]),
    html.Hr(),

    html.Div([
        html.Label("Moving Average Window:", style={'marginRight': '10px'}),
        html.Div(
            dcc.Slider(
                id="ma-window-slider",
                min=0,
                max=len(ma_windows) - 1,
                value=1, # Default to 100
                marks=ma_marks,
                step=None,
            ), style={'width': '200px'}
        ),
    ], style={'display': 'flex', 'alignItems': 'center'}),

    html.Div(id="performance-graphs", className="mt-4")
], fluid=True)


# --- Callback to Update Upload Component Text ---
@app.callback(
    Output('upload-measurement', 'children'),
    [Input('upload-measurement', 'filename')]
)
def update_upload_text(filename):
    if filename:
        return html.Div(f'Selected file: {filename}')
    return html.Div(['Drag and Drop or ', html.A('Select sync_measurement.csv')])


# --- Callback to Update Throughput Graph ---
@app.callback(
    Output("throughput-graph", "figure"),
    [Input("show-throughput-checkbox", "value"),
     Input("ma-window-slider", "value"),
     Input('upload-measurement', 'contents')]
)
def update_throughput_graph(show_throughput_value, window_index, contents):
    df_measurement_local = pd.DataFrame()
    if contents:
        content_type, content_string = contents.split(',')
        decoded = base64.b64decode(content_string)
        try:
            df_measurement_local = pd.read_csv(io.StringIO(decoded.decode('utf-8')), sep=';', low_memory=False)
        except Exception as e:
            print(f"Error parsing measurement uploaded file: {e}")
    elif not df_measurement.empty:
        df_measurement_local = df_measurement.copy()

    if df_measurement_local.empty or not show_throughput_value:
        # Return an empty figure to hide the graph
        fig = go.Figure()
        fig.update_layout(
            template=None,
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False, visible=False),
            yaxis=dict(showgrid=False, zeroline=False, showticklabels=False, visible=False),
            height=10 # Make it very small
        )
        return fig

    df_measurement_local = process_measurement_df(df_measurement_local)
    window = ma_windows[window_index]

    # Calculate moving averages
    df_measurement_local['TPS_ma'] = df_measurement_local['Transactions_per_Second'].rolling(window=window, min_periods=1).mean()
    df_measurement_local['ms_per_Tx_ma'] = df_measurement_local['Time_per_Transaction_ms'].rolling(window=window, min_periods=1).mean()
    df_measurement_local['BPS_ma'] = df_measurement_local['Blocks_per_Second'].rolling(window=window, min_periods=1).mean()

    # Create figure with secondary y-axis
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # Add traces
    fig.add_trace(go.Scatter(
        x=df_measurement_local["SyncInProgressTime_s"], y=df_measurement_local['TPS_ma'], name='Transactions / sec (MA)', mode='lines', line=dict(color='green'),
        customdata=df_measurement_local[['SyncTime_Formatted']],
        hovertemplate=(
            '<b>TPS (MA)</b>: %{y:.2f}<br>' +
            '<b>Time</b>: %{customdata[0]} (%{x:.0f}s)<extra></extra>'
        )
    ), secondary_y=False)
    fig.add_trace(go.Scatter(
        x=df_measurement_local["SyncInProgressTime_s"], y=df_measurement_local['BPS_ma'], name='Blocks / sec (MA)', mode='lines', line=dict(color='blue'),
        hovertemplate='<b>BPS (MA)</b>: %{y:.2f}<extra></extra>'
    ), secondary_y=False)
    fig.add_trace(go.Scatter(
        x=df_measurement_local["SyncInProgressTime_s"], y=df_measurement_local['Blocks_per_Second'], name='Blocks / sec', mode='lines', line=dict(color='lightblue', dash='dot'),
        hovertemplate='<b>BPS</b>: %{y:.2f}<extra></extra>'
    ), secondary_y=False)
    fig.add_trace(go.Scatter(
        x=df_measurement_local["SyncInProgressTime_s"], y=df_measurement_local['ms_per_Tx_ma'], name='ms / Transaction (MA)', mode='lines', line=dict(color='purple', dash='dash'),
        hovertemplate='<b>ms/Tx (MA)</b>: %{y:.2f}<extra></extra>'
    ), secondary_y=True)

    # Update layout and axes
    fig.update_layout(
        title_text=f"Throughput Analysis (MA Window: {window})",
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
    fig.update_yaxes(title_text="<b>Transactions / Blocks / Second</b>", secondary_y=False)
    fig.update_yaxes(title_text="<b>ms / Transaction</b>", secondary_y=True)
    fig.update_xaxes(title_text="Sync in Progress Time [s]")

    return fig

# --- Callback to Update Summary Table ---
@app.callback(
    Output("summary-table-container", "children"),
    [Input('upload-measurement', 'contents')]
)
def update_summary_table(contents):
    df_measurement_local = pd.DataFrame()
    if contents:
        content_type, content_string = contents.split(',')
        decoded = base64.b64decode(content_string)
        try:
            df_measurement_local = pd.read_csv(io.StringIO(decoded.decode('utf-8')), sep=';', low_memory=False)
        except Exception as e:
            print(f"Error parsing measurement uploaded file: {e}")
    elif not df_measurement.empty:
        df_measurement_local = df_measurement.copy()

    if not df_measurement_local.empty:
        summary = df_measurement_local[all_metrics].describe().T[["min", "mean", "50%", "max", "std"]]
        summary.rename(columns={"50%": "median", "std": "stddev"}, inplace=True)
        return dbc.Table.from_dataframe(summary.round(2), striped=True, bordered=True, hover=True)
    else:
        return html.P("No measurement data to display.")

# --- Callback to Update Performance Graphs ---
@app.callback(
    Output("performance-graphs", "children"),
    [Input("ma-window-slider", "value"),
     Input('upload-measurement', 'contents')]
)
def update_graphs(window_index, contents):
    df_measurement_local = pd.DataFrame()
    if contents:
        content_type, content_string = contents.split(',')
        decoded = base64.b64decode(content_string)
        try:
            df_measurement_local = pd.read_csv(io.StringIO(decoded.decode('utf-8')), sep=';', low_memory=False)
        except Exception as e:
            print(f"Error parsing measurement uploaded file: {e}")
    elif not df_measurement.empty:
        df_measurement_local = df_measurement.copy()

    if df_measurement_local.empty:
        return [html.P("Cannot generate graphs because sync_measurement.csv is missing.")]

    df_measurement_local = process_measurement_df(df_measurement_local)
    window = ma_windows[window_index]
    graphs = []

    for metric in time_metrics:
        # Calculate moving average
        df_measurement_local[f"{metric}_ma"] = df_measurement_local[metric].rolling(window=window, min_periods=1).mean()

        # Create figure with secondary y-axis
        fig = make_subplots(specs=[[{"secondary_y": True}]])

        # Add traces for metric, its MA, and Block Height
        fig.add_trace(go.Scatter(
            x=df_measurement_local["SyncInProgressTime_s"], y=df_measurement_local[metric], name=metric, mode='lines', line=dict(width=1, color='grey'),
            customdata=df_measurement_local[['SyncTime_Formatted', f"{metric}_ma", 'Block_height']],
            hovertemplate=(
                f'<b>{metric}</b>: %{{y:.2f}} ms<br>' +
                f'<b>{metric} (MA)</b>: %{{customdata[1]:.2f}} ms<br>' +
                '<b>Block Height</b>: %{customdata[2]}<br>' +
                '<b>Time</b>: %{customdata[0]} (%{x:.0f}s)<extra></extra>'
            )
        ), secondary_y=False)
        fig.add_trace(go.Scatter(
            x=df_measurement_local["SyncInProgressTime_s"], y=df_measurement_local[f"{metric}_ma"], name=f"{metric} MA", mode='lines', line=dict(width=2, color='red'),
            hoverinfo='none'
        ), secondary_y=False)
        fig.add_trace(go.Scatter(
            x=df_measurement_local["SyncInProgressTime_s"], y=df_measurement_local["Block_height"], name="Block Height", mode='lines', line=dict(color='lightblue', dash='dot'),
            hoverinfo='none'
        ), secondary_y=True)

        # Update layout and axes
        fig.update_layout(
            title_text=f"{metric} and Block Height vs. Sync Time (MA Window: {window})",
            height=500,
            legend=dict(orientation="v",
                        yanchor="top",
                        y=-0.2,
                        xanchor="left",
                        x=0,
                        itemclick='toggle',
                        itemdoubleclick='toggleothers'),
            # Add hover lines (crosshairs) for better readability
            hoverlabel=dict(bgcolor="rgba(255, 255, 255, 0.8)", font=dict(color='black')),
            hovermode='x unified',
            xaxis_showspikes=True,
            xaxis_spikemode='across',
            xaxis_spikedash='dot',
            yaxis_showspikes=True,
            yaxis_spikedash='dot',
            yaxis2_showspikes=True,
            yaxis2_spikedash='dot'
        )
        fig.update_yaxes(title_text=f"<b>{metric}</b>", secondary_y=False)
        fig.update_yaxes(title_text="<b>Block Height</b>", secondary_y=True)
        fig.update_xaxes(title_text="Sync in Progress Time [s]")

        graphs.append(html.Div([dcc.Graph(figure=fig)]))

    return graphs


if __name__ == "__main__":
    app.run(debug=True, port=8051)