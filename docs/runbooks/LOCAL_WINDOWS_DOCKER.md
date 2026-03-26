# Running the Silver Pipeline Locally on Windows (Docker)

PySpark cannot run directly on Windows (Hadoop NativeIO linking errors).
Use Docker. This runbook documents the configuration that achieves ~100 s
for a typical production day (~2 100 notices across 10 notice types).

## Prerequisites

- Docker Desktop installed and running (Linux containers mode).
- WSL2 enabled. The `wsl.exe` command must work from PowerShell/Git Bash.
- Docker image built or pulled: `procurement-lakehouse:latest`.
- Bronze data available somewhere accessible from WSL2.

## Why run from WSL2, not Git Bash / PowerShell

Docker bind mounts on Windows filesystem paths (`E:\...`) go through the
Windows kernel ↔ WSL2 hypervisor ↔ Linux boundary and are 2–3× slower than
mounts that stay entirely in WSL2's native ext4 filesystem.

Running the `docker run` command from a WSL2 terminal and mounting paths
under `/home/<user>/` eliminates this bottleneck for data-heavy directories.

Source code directories (`src/`, `scripts/`, `refs/`) are read-mostly and
small, so mounting them directly from the Windows filesystem (`/mnt/e/...`)
is acceptable.

## One-time data setup (copy bronze to WSL2 filesystem)

Open a WSL2 shell and run:

```bash
# Copy only the day you care about (fast, ~11 MB for one day)
mkdir -p ~/bench_bronze/notices
for nt_dir in /mnt/e/git_projects/procurement-watchdog-api-exploration/data/bronze/notices/*/; do
  nt=$(basename "$nt_dir")
  src="$nt_dir/publicationDateDay=2025-04-25"
  [ -d "$src" ] || continue
  mkdir -p ~/bench_bronze/notices/$nt
  cp -r "$src" ~/bench_bronze/notices/$nt/
done
```

Or copy the entire bronze store if you benchmark multiple days:

```bash
cp -r /mnt/e/git_projects/procurement-watchdog-api-exploration/data/bronze/notices \
      ~/bench_bronze/notices
```

## Optimal Docker run command

Run from a WSL2 shell (not PowerShell or Git Bash):

```bash
mkdir -p ~/bench_silver

docker run --rm \
  -v /mnt/e/git_projects/procurement-watchdog-lakehouse/src:/app/src \
  -v /mnt/e/git_projects/procurement-watchdog-lakehouse/scripts:/app/scripts \
  -v /mnt/e/git_projects/procurement-watchdog-lakehouse/refs:/app/refs:ro \
  -v ~/bench_bronze:/data/bronze:ro \
  -v ~/bench_silver:/data/silver \
  procurement-lakehouse:latest \
  python scripts/pipeline/build_silver_day.py 2025-04-25 \
    --bronze-dir /data/bronze \
    --silver-dir /data/silver \
    --shuffle-partitions 8 \
    --max-batch-workers 6 \
    --profile-json /data/silver/bench_profile.json
```

### Key flags

| Flag | Value | Why |
|---|---|---|
| `--shuffle-partitions` | `8` | Default (16) creates too many partitions for 2 k rows; halving it cuts ContractNotice write time by ~60 %. |
| `--max-batch-workers` | `6` | Runs all 6 light notice types simultaneously in wave 1, warming the JVM fully before the 4 heavy types start. |
| `--profile-json` | any path | Optional; writes per-batch timing for post-run analysis. |

## Invoking from Git Bash / PowerShell (slower path)

If you must run from outside WSL2, prefix with `MSYS_NO_PATHCONV=1` (Git Bash
only) to prevent path mangling, and use Windows-style mounts. Expect ~2× longer
wall time due to bind-mount I/O overhead.

```bash
# Git Bash — note MSYS_NO_PATHCONV=1
MSYS_NO_PATHCONV=1 docker run --rm \
  -v /e/git_projects/procurement-watchdog-lakehouse/src:/app/src \
  -v /e/git_projects/procurement-watchdog-lakehouse/scripts:/app/scripts \
  -v /e/git_projects/procurement-watchdog-lakehouse/refs:/app/refs:ro \
  -v /e/git_projects/procurement-watchdog-api-exploration/data/bronze:/data/bronze:ro \
  -v /e/git_projects/procurement-watchdog-api-exploration/data/silver:/data/silver \
  procurement-lakehouse:latest \
  python scripts/pipeline/build_silver_day.py 2025-04-25 \
    --bronze-dir /data/bronze \
    --silver-dir /data/silver \
    --shuffle-partitions 8 \
    --max-batch-workers 6
```

## Observed timings (2025-04-25, 2099 notices, 10 notice types)

| Configuration | Total | ContractNotice batch |
|---|---|---|
| Default (sp=16, workers=4), Windows mounts | ~205 s | 116 s |
| sp=8, workers=4, WSL2 native mounts | ~107 s | 57 s |
| sp=8, workers=6, WSL2 native mounts | **~100 s** | 59 s |

## Troubleshooting

**`FileNotFoundError: /app/refs/bzp_api/SL.MO.013.json`**
The Dockerfile does not bundle `refs/`. Always include the
`-v .../refs:/app/refs:ro` volume mount.

**`ValueError: Raw file not found: data/raw/bzp_*.json`**
The pipeline fell back to raw JSON mode because no matching Bronze partition
was found. Check that the bronze path contains
`notices/noticeType=<X>/publicationDateDay=<DATE>/` sub-directories.

**Path mangled to `C:/Program Files/Git/data/silver`**
You are running from Git Bash without `MSYS_NO_PATHCONV=1`. Add it as a
prefix to the `docker run` command or switch to a WSL2 shell.
