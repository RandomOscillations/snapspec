param(
    [switch]$SkipBuild,
    [switch]$SkipDebug,
    [switch]$SkipSweeps,
    [switch]$SkipBaseline,
    [switch]$LeaveWorkersUp
)

$ErrorActionPreference = "Stop"

function Invoke-CoordinatorRun {
    param(
        [hashtable]$EnvVars,
        [string[]]$CommandArgs
    )

    $argList = @("compose", "run", "--rm")
    foreach ($key in $EnvVars.Keys) {
        $argList += "-e"
        $argList += "$key=$($EnvVars[$key])"
    }
    $argList += "coordinator"
    $argList += $CommandArgs

    & docker @argList
}

function Run-Distributed {
    param(
        [string]$Experiment,
        [string]$ParamValue,
        [string]$Strategy,
        [string]$Duration,
        [string]$SnapshotInterval,
        [string]$WriteRate,
        [string]$CrossNodeRatio,
        [string]$EffectDelayMs = "0",
        [string]$ValidationDelayMs = "0",
        [string]$ConfigPrefix = "row",
        [string]$Rep = "1",
        [string]$OutputDir = "/app/results",
        [string]$TotalTokens = "100000",
        [string]$TotalBlocks = "256",
        [string]$Seed = "42"
    )

    $envVars = @{
        SNAPSPEC_EXPERIMENT          = $Experiment
        SNAPSPEC_CONFIG_PREFIX       = $ConfigPrefix
        SNAPSPEC_PARAM_VALUE         = $ParamValue
        SNAPSPEC_REP                 = $Rep
        SNAPSPEC_OUTPUT_DIR          = $OutputDir
        SNAPSPEC_DURATION            = $Duration
        SNAPSPEC_SNAPSHOT_INTERVAL   = $SnapshotInterval
        SNAPSPEC_WRITE_RATE          = $WriteRate
        SNAPSPEC_CROSS_NODE_RATIO    = $CrossNodeRatio
        SNAPSPEC_EFFECT_DELAY_MS     = $EffectDelayMs
        SNAPSPEC_VALIDATION_DELAY_MS = $ValidationDelayMs
        SNAPSPEC_TOTAL_TOKENS        = $TotalTokens
        SNAPSPEC_TOTAL_BLOCKS        = $TotalBlocks
        SNAPSPEC_SEED                = $Seed
        SNAPSPEC_STRATEGY            = $Strategy
    }

    Invoke-CoordinatorRun -EnvVars $envVars -CommandArgs @()
}

function Run-Sweep {
    param(
        [string[]]$Args
    )

    Invoke-CoordinatorRun -EnvVars @{} -CommandArgs @("python", "experiments/run_vm_sweep.py") + $Args
}

function Run-Analysis {
    param(
        [string]$Experiment,
        [string]$Metric = "avg_throughput_writes_sec"
    )

    Invoke-CoordinatorRun -EnvVars @{} -CommandArgs @(
        "python", "experiments/analyze_vm_results.py",
        "--results-dir", "/app/results",
        "--experiment", $Experiment,
        "--metric", $Metric
    )
}

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $scriptDir

New-Item -ItemType Directory -Force -Path "state/node0/data", "state/node0/archives" | Out-Null
New-Item -ItemType Directory -Force -Path "state/node1/data", "state/node1/archives" | Out-Null
New-Item -ItemType Directory -Force -Path "state/node2/data", "state/node2/archives" | Out-Null
New-Item -ItemType Directory -Force -Path "../results" | Out-Null

try {
    if (-not $SkipBuild) {
        Write-Host "Building Docker images..."
        docker compose build
    }

    Write-Host "Starting worker containers..."
    docker compose up -d node0 node1 node2
    Start-Sleep -Seconds 2

    if (-not $SkipDebug) {
        Write-Host ""
        Write-Host "=== Debug Conservation: Two-Phase ==="
        Run-Distributed `
            -Experiment "debug_conservation" `
            -ParamValue "0.20" `
            -Strategy "two_phase" `
            -Duration "20" `
            -SnapshotInterval "1" `
            -WriteRate "350" `
            -CrossNodeRatio "0.20" `
            -EffectDelayMs "25" `
            -ValidationDelayMs "50"

        Write-Host ""
        Write-Host "=== Debug Conservation: Speculative ==="
        Run-Distributed `
            -Experiment "debug_conservation" `
            -ParamValue "0.20" `
            -Strategy "speculative" `
            -Duration "20" `
            -SnapshotInterval "1" `
            -WriteRate "350" `
            -CrossNodeRatio "0.20" `
            -EffectDelayMs "25" `
            -ValidationDelayMs "50"
    }

    if (-not $SkipSweeps) {
        Write-Host ""
        Write-Host "=== Experiment 3: Dependency Sweep ==="
        Run-Sweep -Args @(
            "--experiment", "dependency",
            "--nodes", "0:node0:9000,1:node1:9000,2:node2:9000",
            "--output-dir", "/app/results",
            "--duration", "20",
            "--write-rate", "350",
            "--snapshot-interval", "1",
            "--effect-delay-ms", "25",
            "--validation-delay-ms", "50",
            "--total-tokens", "100000",
            "--total-blocks", "256",
            "--strategies", "two_phase", "speculative"
        )
        Run-Analysis -Experiment "exp3_dependency"

        Write-Host ""
        Write-Host "=== Experiment 1: Frequency Sweep ==="
        Run-Sweep -Args @(
            "--experiment", "frequency",
            "--nodes", "0:node0:9000,1:node1:9000,2:node2:9000",
            "--output-dir", "/app/results",
            "--duration", "15",
            "--write-rate", "200",
            "--cross-node-ratio", "0.10",
            "--total-tokens", "100000",
            "--total-blocks", "256"
        )
        Run-Analysis -Experiment "exp1_frequency"
    }

    if (-not $SkipBaseline) {
        Write-Host ""
        Write-Host "=== Baseline C3/C4/C5 ==="

        Run-Distributed `
            -Experiment "vm_c3" `
            -ParamValue "baseline" `
            -Strategy "pause_and_snap" `
            -Duration "15" `
            -SnapshotInterval "5" `
            -WriteRate "200" `
            -CrossNodeRatio "0.10"

        Run-Distributed `
            -Experiment "vm_c4" `
            -ParamValue "baseline" `
            -Strategy "two_phase" `
            -Duration "15" `
            -SnapshotInterval "5" `
            -WriteRate "200" `
            -CrossNodeRatio "0.10"

        Run-Distributed `
            -Experiment "vm_c5" `
            -ParamValue "baseline" `
            -Strategy "speculative" `
            -Duration "15" `
            -SnapshotInterval "5" `
            -WriteRate "200" `
            -CrossNodeRatio "0.10"
    }
}
finally {
    if (-not $LeaveWorkersUp) {
        Write-Host ""
        Write-Host "Stopping containers..."
        docker compose down
    }
    else {
        Write-Host ""
        Write-Host "Workers left running because -LeaveWorkersUp was set."
    }
}
