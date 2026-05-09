<#
.SYNOPSIS
    Fetch AirBnB calendar availability for known listings.

.DESCRIPTION
    Activates the project virtual environment (if present), then invokes the
    AirBnB availability pipeline to fetch per-date availability data via the
    AirBnB internal calendar API and store the results in the
    airbnb_availability_bronze dataset (Parquet, queryable via DuckDB).

    Listing IDs are discovered automatically from the airbnb_bronze dataset
    (produced by run_airbnb.ps1) unless --FacilityId is specified.

.PARAMETER DataPath
    Root directory for the data lake. Defaults to "data" (relative to this
    script's location).

.PARAMETER FacilityId
    One or more explicit listing IDs to fetch. Can be repeated. If omitted,
    all IDs from airbnb_bronze are used.

.PARAMETER MaxListings
    Maximum number of listings to process in this run.

.PARAMETER Months
    Number of calendar months to fetch per listing (1-12). Defaults to 6.

.PARAMETER Delay
    Polite delay in seconds between API calls. Defaults to 2.0.

.PARAMETER Visible
    Run the browser in visible (non-headless) mode.

.PARAMETER NoSkip
    Re-fetch listings even if today's data already exists.

.PARAMETER LogLevel
    Logging verbosity passed to the Python pipeline. Defaults to INFO.

.EXAMPLE
    .\run_airbnb_availability.ps1

.EXAMPLE
    .\run_airbnb_availability.ps1 -Months 3 -MaxListings 50

.EXAMPLE
    .\run_airbnb_availability.ps1 -FacilityId 12345 -FacilityId 67890
#>

[CmdletBinding()]
param(
    [Parameter(HelpMessage = "Root directory for the data lake. Defaults to 'data'.")]
    [string] $DataPath = "data",

    [Parameter(HelpMessage = "Explicit listing ID(s) to fetch. Repeatable.")]
    [string[]] $FacilityId = @(),

    [Parameter(HelpMessage = "Maximum number of listings to process.")]
    [int] $MaxListings = 0,

    [Parameter(HelpMessage = "Calendar months to fetch per listing (1-12). Defaults to 6.")]
    [ValidateRange(1, 12)]
    [int] $Months = 6,

    [Parameter(HelpMessage = "Delay in seconds between API calls. Defaults to 2.0.")]
    [ValidateRange(0.0, 120.0)]
    [double] $Delay = 2.0,

    [Parameter(HelpMessage = "Run browser in visible mode.")]
    [switch] $Visible,

    [Parameter(HelpMessage = "Re-fetch even if today's data already exists.")]
    [switch] $NoSkip,

    [Parameter(HelpMessage = "Logging verbosity (DEBUG, INFO, WARNING, ERROR).")]
    [ValidateSet("DEBUG", "INFO", "WARNING", "ERROR")]
    [string] $LogLevel = "INFO"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# ---------------------------------------------------------------------------
# Execution timestamp
# ---------------------------------------------------------------------------

$RunTs     = Get-Date -Format 'yyyyMMdd-HHmmss'
$RunTsFile = $RunTs -replace '-', ''

# ---------------------------------------------------------------------------
# Transcript (PS1 log)
# ---------------------------------------------------------------------------

$ScriptName = [System.IO.Path]::GetFileNameWithoutExtension($MyInvocation.MyCommand.Name)
$LogDir     = Join-Path (Split-Path -Parent $MyInvocation.MyCommand.Definition) "logs\$ScriptName\$RunTs"
$LogFile    = Join-Path $LogDir "${ScriptName}_log_${RunTsFile}.log"

New-Item -ItemType Directory -Path $LogDir -Force | Out-Null
Start-Transcript -Path $LogFile -Append | Out-Null

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

function Write-Info ([string]$Msg) { Write-Host "[airbnb-availability] $Msg" -ForegroundColor Cyan }
function Write-Warn ([string]$Msg) { Write-Host "[airbnb-availability] WARNING: $Msg" -ForegroundColor Yellow }
function Write-Fail ([string]$Msg) { Write-Host "[airbnb-availability] ERROR: $Msg" -ForegroundColor Red }

Write-Info "Transcript started: $LogFile"

# ---------------------------------------------------------------------------
# Locate repository root
# ---------------------------------------------------------------------------

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Definition

# ---------------------------------------------------------------------------
# Virtual environment activation
# ---------------------------------------------------------------------------

$VenvActivate = Join-Path $ScriptDir ".venv\Scripts\Activate.ps1"

if (Test-Path $VenvActivate) {
    Write-Info "Activating virtual environment: $VenvActivate"
    & $VenvActivate
} else {
    Write-Warn ".venv not found at '$ScriptDir\.venv'. Falling back to system Python."
}

# ---------------------------------------------------------------------------
# Verify Python is available
# ---------------------------------------------------------------------------

$PythonCmd = Get-Command python -ErrorAction SilentlyContinue

if ($null -eq $PythonCmd) {
    Write-Fail "python not found on PATH. Install Python 3.10+ or activate the project venv."
    exit 1
}

Write-Info "Using Python: $($PythonCmd.Source)"

# ---------------------------------------------------------------------------
# Resolve DataPath
# ---------------------------------------------------------------------------

if (-not [System.IO.Path]::IsPathRooted($DataPath)) {
    $DataPath = Join-Path $ScriptDir $DataPath
}

# ---------------------------------------------------------------------------
# Build argument list
# ---------------------------------------------------------------------------

$PipelineArgs = @(
    "-m", "bookinz.pipeline.airbnb_availability_pipeline",
    "--data-path", $DataPath,
    "--months",    $Months,
    "--delay",     $Delay,
    "--log-level", $LogLevel
)

foreach ($id in $FacilityId) {
    $PipelineArgs += @("--facility-id", $id)
}

if ($MaxListings -gt 0) {
    $PipelineArgs += @("--max-listings", $MaxListings)
}

if ($Visible) {
    $PipelineArgs += "--visible"
}

if ($NoSkip) {
    $PipelineArgs += "--no-skip"
}

Write-Info "Data lake root : $DataPath"
Write-Info "Months ahead   : $Months"
Write-Info "Delay          : $($Delay)s"
if ($FacilityId.Count -gt 0) { Write-Info "Facility IDs   : $($FacilityId -join ', ')" }
if ($MaxListings -gt 0)       { Write-Info "Max listings   : $MaxListings" }
Write-Info "Visible        : $($Visible.IsPresent)"
Write-Info "Skip existing  : $(-not $NoSkip.IsPresent)"
Write-Host ""

python @PipelineArgs

$ExitCode = $LASTEXITCODE

Write-Host ""
if ($ExitCode -eq 0) {
    Write-Info "Availability pipeline completed successfully."
} else {
    Write-Fail "Availability pipeline exited with code $ExitCode."
}

Write-Info "Transcript saved: $LogFile"
Stop-Transcript | Out-Null

exit $ExitCode
