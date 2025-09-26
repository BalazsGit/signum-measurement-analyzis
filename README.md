# Sync Progress Analyzer Tool

The `signum-node` project includes a built-in, browser-based visualization tool, the `Sync Progress Analyzer`, designed to analyze the node's synchronization logs (`sync_progress.csv` and `sync_measurement.csv`). This tool is written in Python using the Dash framework and allows users to conduct in-depth analysis and comparison of synchronization performance.

The tool is located in the `signum-measurement-analyzis-tool/sync_progress_analyzis_tool/` directory.

## Why Use This Tool?

Understanding the synchronization performance of a `signum-node` is crucial for optimization and troubleshooting. This tool provides a powerful and user-friendly interface to:

-   **Benchmark Hardware:** Quantify the impact of hardware changes (e.g., switching from HDD to SSD, upgrading CPU) on sync speed.
-   **Evaluate Software Updates:** Compare performance before and after a node software update to identify regressions or improvements.
-   **Identify Bottlenecks:** Pinpoint specific block ranges where synchronization slows down, helping to diagnose potential network or system issues.
-   **Share Findings:** Easily generate and share comprehensive HTML reports of your analysis with others.

## Key Features

- **Interactive Graphs**: Displays block height and synchronization speed (Blocks/sec) over time.
- **Comparative Analysis**: Provides the ability to load and compare two log files side-by-side, which is ideal for measuring the impact of hardware or software changes.
- **Detailed Metrics**: Calculates and displays key performance indicators (KPIs) in a tabular format, such as total sync time, average/median/maximum speed, standard deviation, and skewness.
- **Moving Average**: Smooths the speed data using a moving average with an adjustable window size for better visibility of trends.
- **Data Filtering**: Allows filtering the data to a specific block range for more detailed analysis.
- **Metadata Management**: Automatically reads and displays system information (e.g., software version, hardware) found in the log file headers, and allows for editing and adding new data.
- **Data Management**:
    - Option to clear data in memory ("Clear CSV") to improve the handling of large files.
    - Save modified (filtered or metadata-enriched) data to a new CSV file.
- **Report Generation**: Saves the current view as a single, self-contained HTML file that can be shared and preserved.
- **User-Friendly Interface**: Switch between light and dark themes, and built-in help (info icons) for all features.
- **Automatic Dependency Management**: Attempts to update the necessary Python packages upon startup.

## Getting Started

### Prerequisites

- **Python 3**: Ensure that Python 3 is installed on your system.
- **pip**: The Python package manager is required to install dependencies.

### Recommended Launch on Windows (Portable Setup)

For Windows users, the easiest way to run the tool is by using the provided startup script, which automates the entire setup process. This method does not require a pre-existing Python installation.

1.  Double-click the `start_sync_progress_analyzer.bat` file.

This batch file executes the `start_sync_progress_analyzer.ps1` PowerShell script, which performs the following steps automatically:
-   **Portable Python**: It checks for a local, portable Python installation within the tool's `python` subdirectory. If it's not found, it downloads and unpacks a compatible version.
-   **Dependency Management**: It ensures `pip` (the Python package manager) is available and then installs or updates all required libraries (`dash`, `pandas`, `plotly`, etc.).
-   **Application Launch**: Once the environment is ready, it automatically starts the `sync_progress_analyzer.py` application.

This is the recommended method for Windows as it provides a self-contained environment and ensures that all dependencies are correct and up-to-date on every launch.

### Manual Launch (All Platforms)

### Launching the Tool

1.  Navigate to the tool's directory in your terminal:
    ```bash
    cd signum-measurement-analyzis-tool/sync_progress_analyzis_tool/
    ```

2.  Run the script using Python:
    ```bash
    python sync_progress_analyzer.py
    ```

3.  Upon startup, the script will attempt to install or update the required `dash`, `pandas`, and `plotly` packages.

4.  After a successful launch, a message will appear in the terminal indicating that the Dash application is running, usually at `http://127.0.0.1:8050/`. Open this link in your browser.

## Using the Tool

1.  **Loading Data Files**:
    - The tool automatically attempts to load the `measurements/sync_progress.csv` file.
    - You can upload new files by clicking on the "Drag and Drop or Select..." areas. The "Original" file is the main source for analysis, while the "Comparison" file is an optional second file for comparison.

2.  **Analysis**:
    - Examine the synchronization progress on the main graph. You can zoom in on specific periods of interest with the mouse.
    - The "Metrics Summary" table provides a comprehensive overview of performance. If a comparison file is loaded, it also displays the differences and the direction of performance change (better/worse).
    - Use the "Moving Average Window" slider to fine-tune the smoothing of the speed curve.

3.  **Modifying and Saving Data**:
    - On the "System Info" cards, you can edit existing metadata or add new entries.
    - With the "Save" or "Save As..." buttons, you can save the filtered and/or metadata-enriched data to a new CSV file in the `measurements/saved/` directory.

4.  **Saving a Report**:
    - By clicking the save icon in the upper-right corner, you can save the entire page as a single HTML file (without interactive elements) into the `reports/` directory.

## Troubleshooting

-   **Application does not start:** Ensure you have Python 3 and `pip` installed and accessible from your terminal's PATH.
-   **"No such file or directory" on startup:** The tool expects to find a `measurements` directory with a `sync_progress.csv` file by default. If your logs are elsewhere, you can ignore this initial error and upload your file manually.
-   **Browser does not open automatically:** Manually copy the URL shown in the terminal (e.g., `http://127.0.0.1:8050/`) and paste it into your web browser.

## Contributing

Contributions are welcome! If you have suggestions for improvements or encounter any bugs, please feel free to open an issue or submit a pull request on the project's GitHub repository.

When reporting a bug, please include:
-   The version of the tool you are using.
-   Steps to reproduce the issue.
-   Any relevant error messages from the terminal.

## License

This tool is part of the `signum-node` project and is released under the GPLv3 License.