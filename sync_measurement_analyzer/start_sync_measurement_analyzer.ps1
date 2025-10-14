# --------------------------------------
# PowerShell portable Python setup + run
# --------------------------------------

# Relative paths
$BaseDir = Split-Path -Parent $MyInvocation.MyCommand.Definition
Write-Host "Base directory: $BaseDir"
$PythonDir = Join-Path $BaseDir "python"
$PythonExe = Join-Path $PythonDir "python.exe"
$PythonZipUrl = "https://www.python.org/ftp/python/3.12.2/python-3.12.2-embed-amd64.zip"
$PythonZip = Join-Path $BaseDir "python-embed.zip"

# 1. Download Python if it doesn't exist
Write-Host "Checking for Python at $PythonExe..."
if (-not (Test-Path $PythonExe)) {
    Write-Host "Python not found. Downloading portable Python..."
    Invoke-WebRequest -Uri $PythonZipUrl -OutFile $PythonZip
    Write-Host "Download complete."

    # Unpacking
    Write-Host "Unpacking Python to $PythonDir..."
    Expand-Archive -Path $PythonZip -DestinationPath $PythonDir -Force
    Remove-Item $PythonZip
    Write-Host "Python setup complete."
} else {
    Write-Host "Python already exists."
}

# Enable site-packages in the embedded distribution by editing the ._pth file.
# This is necessary for pip and installed packages to be found.
# This is run every time to fix potentially broken states.
$PthFile = Join-Path $PythonDir "python312._pth"
if (Test-Path $PthFile) {
    Write-Host "Ensuring site-packages are enabled for embedded Python in $PthFile..."
    (Get-Content $PthFile) -replace '#import site', 'import site' | Set-Content $PthFile
}

# 2. Install pip if it doesn't exist
$PipExe = Join-Path $PythonDir "Scripts\pip.exe"
Write-Host "Checking for pip at $PipExe..."
if (-not (Test-Path $PipExe)) {
    Write-Host "pip not found. Downloading and installing get-pip.py..."
    $GetPip = Join-Path $BaseDir "get-pip.py"
    Invoke-WebRequest -Uri "https://bootstrap.pypa.io/get-pip.py" -OutFile $GetPip
    & $PythonExe $GetPip
    Remove-Item $GetPip
    Write-Host "pip installation complete."
} else {
    Write-Host "pip already exists."
}

# 3. Install required packages
Write-Host "Installing/updating required Python packages..."
$Packages = @("pandas", "dash", "dash-bootstrap-components", "plotly")
foreach ($pkg in $Packages) {
    Write-Host " - Installing $pkg..."
    & $PythonExe -m pip install --upgrade $pkg
}
Write-Host "All packages are up to date."

# 4. Run analyzer script
$ScriptPath = Join-Path $BaseDir "sync_measurement_analyzer.py"
Write-Host "Running analyzer script: $ScriptPath"
& $PythonExe $ScriptPath
