<#
.SYNOPSIS
    Scrape booking.com accommodations for a given location and date range.

.DESCRIPTION
    Activates the project virtual environment (if present), then invokes the
    Bookinz pipeline to scrape accommodations from booking.com and store the
    results in the bronze layer (Parquet files on disk, queryable via DuckDB).

.PARAMETER Location
    City or region to search (e.g. "Amsterdam", "New York").

.PARAMETER CheckIn
    Check-in date in YYYY-MM-DD format.

.PARAMETER CheckOut
    Check-out date in YYYY-MM-DD format.

.PARAMETER DataPath
    Root directory for the data lake. Defaults to "data" (relative to this
    script's location).

.PARAMETER NumAdults
    Number of adult guests. Defaults to 2.

.PARAMETER MaxPages
    Maximum result pages to scrape per area. Defaults to 5.

.PARAMETER Delay
    Polite delay in seconds between HTTP requests. Defaults to 2.0.

.EXAMPLE
    .\run_scraper.ps1 -Location Amsterdam -CheckIn 2026-05-10 -CheckOut 2026-05-12

.EXAMPLE
    .\run_scraper.ps1 -Location Paris -CheckIn 2026-06-01 -CheckOut 2026-06-05 -NumAdults 3 -MaxPages 2 -Delay 1.5
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true, HelpMessage = "City or region to search (e.g. Amsterdam).")]
    [ValidateNotNullOrEmpty()]
    [string] $Location,

    [Parameter(Mandatory = $true, HelpMessage = "Check-in date (YYYY-MM-DD).")]
    [ValidateNotNullOrEmpty()]
    [string] $CheckIn,

    [Parameter(Mandatory = $true, HelpMessage = "Check-out date (YYYY-MM-DD).")]
    [ValidateNotNullOrEmpty()]
    [string] $CheckOut,

    [Parameter(HelpMessage = "Root directory for the data lake. Defaults to 'data'.")]
    [string] $DataPath = "data",

    [Parameter(HelpMessage = "Number of adult guests. Defaults to 2.")]
    [ValidateRange(1, 30)]
    [int] $NumAdults = 2,

    [Parameter(HelpMessage = "Maximum result pages to scrape per area. Defaults to 5.")]
    [ValidateRange(1, 100)]
    [int] $MaxPages = 5,

    [Parameter(HelpMessage = "Delay in seconds between HTTP requests. Defaults to 2.0.")]
    [ValidateRange(0.0, 60.0)]
    [double] $Delay = 2.0
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# ---------------------------------------------------------------------------
# Execution timestamp (used for both the log directory and file name)
# ---------------------------------------------------------------------------

$RunTs     = Get-Date -Format 'yyyyMMdd-HHmmss'   # folder:  yyyyMMdd-HHmmss
$RunTsFile = $RunTs -replace '-', ''              # filename: yyyyMMddHHmmss

# ---------------------------------------------------------------------------
# Transcript (PS1 log)
# ---------------------------------------------------------------------------

$ScriptName   = [System.IO.Path]::GetFileNameWithoutExtension($MyInvocation.MyCommand.Name)
$LogDir       = Join-Path (Split-Path -Parent $MyInvocation.MyCommand.Definition) "logs\$ScriptName\$RunTs"
$LogFile      = Join-Path $LogDir "${ScriptName}_log_${RunTsFile}.log"

New-Item -ItemType Directory -Path $LogDir -Force | Out-Null
Start-Transcript -Path $LogFile -Append | Out-Null

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

function Write-Info  ([string]$Msg) { Write-Host "[bookinz] $Msg" -ForegroundColor Cyan }
function Write-Warn  ([string]$Msg) { Write-Host "[bookinz] WARNING: $Msg" -ForegroundColor Yellow }
function Write-Fail  ([string]$Msg) { Write-Host "[bookinz] ERROR: $Msg" -ForegroundColor Red }

Write-Info "Transcript started: $LogFile"

# ---------------------------------------------------------------------------
# Input validation
# ---------------------------------------------------------------------------

$DatePattern = '^\d{4}-\d{2}-\d{2}$'

if ($CheckIn -notmatch $DatePattern) {
    Write-Fail "CheckIn '$CheckIn' is not in YYYY-MM-DD format."
    exit 1
}

if ($CheckOut -notmatch $DatePattern) {
    Write-Fail "CheckOut '$CheckOut' is not in YYYY-MM-DD format."
    exit 1
}

# Verify logical order of dates
$CheckInDate  = [datetime]::ParseExact($CheckIn,  'yyyy-MM-dd', $null)
$CheckOutDate = [datetime]::ParseExact($CheckOut, 'yyyy-MM-dd', $null)

if ($CheckOutDate -le $CheckInDate) {
    Write-Fail "CheckOut ($CheckOut) must be after CheckIn ($CheckIn)."
    exit 1
}

# ---------------------------------------------------------------------------
# Locate repository root (directory containing this script)
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
    Write-Warn "To create a venv: python -m venv .venv  then  pip install -e .[dev]"
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
# Resolve DataPath (make absolute so the pipeline always writes to the right place)
# ---------------------------------------------------------------------------

if (-not [System.IO.Path]::IsPathRooted($DataPath)) {
    $DataPath = Join-Path $ScriptDir $DataPath
}

# ---------------------------------------------------------------------------
# Build argument list and run the pipeline
# ---------------------------------------------------------------------------

$PipelineArgs = @(
    "-m", "bookinz.pipeline.booking_pipeline",
    "--area",       $Location,
    "--checkin",    $CheckIn,
    "--checkout",   $CheckOut,
    "--data-path",  $DataPath,
    "--adults",     $NumAdults,
    "--max-pages",  $MaxPages,
    "--delay",      $Delay
)

Write-Info "Starting scrape - Location: '$Location' | CheckIn: $CheckIn | CheckOut: $CheckOut"
Write-Info "Data lake root : $DataPath"
Write-Info "Guests: $NumAdults | Max pages: $MaxPages | Request delay: $($Delay)s"
Write-Host ""

python @PipelineArgs

$ExitCode = $LASTEXITCODE

Write-Host ""
if ($ExitCode -eq 0) {
    Write-Info "Pipeline completed successfully."
} else {
    Write-Fail "Pipeline exited with code $ExitCode."
}

Write-Info "Transcript saved: $LogFile"
Stop-Transcript | Out-Null

exit $ExitCode
