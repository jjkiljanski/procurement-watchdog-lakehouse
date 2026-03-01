param(
    [string]$LakehouseRoot = "E:\git_projects\procurement-watchdog-lakehouse",
    [string]$ExternalRoot = "E:\git_projects\procurement-watchdog-api-exploration",
    [string]$CaseTargetDate = "2025-06-30",
    [string]$SparkMaster = "local[*]",
    [string]$SilverDirInContainer = "/ext/data/silver",
    [string]$CaseOutputDirInContainer = "/ext/data/silver/case_derived_facts",
    [string]$EuLookupParquetInContainer = "refs/eu_countries.parquet",
    [string]$SilverBackfillStatePathInContainer = "/ext/data/silver/_state/silver_backfill_2025-04-01_2025-12-31.json",
    [string]$SilverBackfillStartDate = "2025-04-01",
    [string]$SilverBackfillEndDate = "2025-12-31",
    [int]$HeartbeatSeconds = 60,
    [int]$Step1TimeoutMinutes = 180
)

$ErrorActionPreference = "Stop"

$logDir = Join-Path $LakehouseRoot "logs"
New-Item -ItemType Directory -Path $logDir -Force | Out-Null

$runTs = Get-Date -Format "yyyyMMdd_HHmmss"
$chainLog = Join-Path $logDir "chain_case_derived_then_resume_silver_$runTs.log"
$step1Stdout = Join-Path $logDir "step1_case_derived_$runTs.out.log"
$step1Stderr = Join-Path $logDir "step1_case_derived_$runTs.err.log"
$step2Start = Join-Path $logDir "step2_silver_resume_$runTs.log"

function Write-Log([string]$msg) {
    $ts = (Get-Date).ToString("s")
    "$ts $msg" | Tee-Object -FilePath $chainLog -Append
}

Write-Log "CHAIN_START"
Write-Log "CHAIN_LOG=$chainLog"
Write-Log "STEP1_STDOUT=$step1Stdout"
Write-Log "STEP1_STDERR=$step1Stderr"
Write-Log "STEP2_LOG=$step2Start"

$caseContainerName = "case-derived-h1-2025-$runTs"
$silverContainerName = "silver-backfill-2025-resume-$runTs"

$step1Args = @(
    "run", "--rm",
    "--name", $caseContainerName,
    "-v", "${LakehouseRoot}:/app",
    "-v", "${ExternalRoot}:/ext",
    "-w", "/app",
    "procurement-pipeline",
    "python", "scripts/pipeline/build_case_derived_facts.py", $CaseTargetDate,
    "--mode", "full",
    "--silver-dir", $SilverDirInContainer,
    "--output-dir", $CaseOutputDirInContainer,
    "--spark-master", $SparkMaster,
    "--shard-count", "1",
    "--eu-lookup-parquet", $EuLookupParquetInContainer
)

Write-Log "STEP1_START container=$caseContainerName asOfDate=$CaseTargetDate"
$step1Started = Get-Date
$step1Proc = Start-Process -FilePath "docker" `
    -ArgumentList $step1Args `
    -RedirectStandardOutput $step1Stdout `
    -RedirectStandardError $step1Stderr `
    -PassThru -NoNewWindow

while (-not $step1Proc.HasExited) {
    Start-Sleep -Seconds $HeartbeatSeconds
    $elapsed = [int]((Get-Date) - $step1Started).TotalSeconds
    Write-Log "STEP1_HEARTBEAT elapsed_sec=$elapsed pid=$($step1Proc.Id)"
    if (((Get-Date) - $step1Started).TotalMinutes -ge $Step1TimeoutMinutes) {
        Write-Log "STEP1_TIMEOUT minutes=$Step1TimeoutMinutes; attempting stop container=$caseContainerName"
        & docker stop $caseContainerName *> $null
        try { Stop-Process -Id $step1Proc.Id -Force -ErrorAction SilentlyContinue } catch {}
        break
    }
}

$step1Proc.Refresh()
$step1Exit = $step1Proc.ExitCode
$step1Elapsed = [int]((Get-Date) - $step1Started).TotalSeconds
Write-Log "STEP1_EXIT code=$step1Exit elapsed_sec=$step1Elapsed"
if ($step1Exit -ne 0) {
    Write-Log "CHAIN_ABORT reason=step1_failed"
    exit $step1Exit
}

Write-Log "STEP2_START container=$silverContainerName"
$step2Args = @(
    "run", "-d",
    "--name", $silverContainerName,
    "-v", "${LakehouseRoot}:/app",
    "-v", "${ExternalRoot}:/ext",
    "-w", "/app",
    "procurement-pipeline",
    "python", "scripts/pipeline/build_silver_backfill.py",
    "--start-date", $SilverBackfillStartDate,
    "--end-date", $SilverBackfillEndDate,
    "--bronze-dir", "/ext/data/bronze",
    "--silver-dir", "/ext/data/silver",
    "--state-path", $SilverBackfillStatePathInContainer,
    "--spark-master", $SparkMaster,
    "--lock-stale-minutes", "10"
)

$step2ContainerId = (& docker @step2Args).Trim()
$step2Exit = $LASTEXITCODE
"container_id=$step2ContainerId`ncontainer_name=$silverContainerName" | Set-Content -Path $step2Start -Encoding UTF8
Write-Log "STEP2_EXIT code=$step2Exit container_id=$step2ContainerId container_name=$silverContainerName"
if ($step2Exit -ne 0) {
    Write-Log "CHAIN_ABORT reason=step2_failed"
    exit $step2Exit
}

Write-Log "CHAIN_DONE"
