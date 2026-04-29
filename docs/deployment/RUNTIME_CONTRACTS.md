# Runtime Contracts (GCP Images)

This repository exposes two deployable Docker images:

- `Dockerfile.spark` — Dataproc Serverless batch container (all pipeline scripts)
- `Dockerfile.downloader` — Cloud Run Job container (BZP API fetch)

Core transformation logic lives in `scripts/pipeline/*` and `src/procurement/*`.

---

## Spark Image (`Dockerfile.spark`)

Used by Dataproc Serverless batches for all pipeline stages.

Entrypoint: scripts are passed as arguments via the Dataproc batch config
(`main_python_file_uri` in Cloud Workflows).

Scripts invoked:
- `scripts/pipeline/fetch_bzp_range.py` (backfill fetch step)
- `scripts/pipeline/build_bronze.py` / `build_bronze_range.py`
- `scripts/pipeline/build_silver_day.py` / `build_silver_range.py`
- `scripts/pipeline/build_silver_update_deltas.py`

---

## Downloader Image (`Dockerfile.downloader`)

Entrypoint: `python apps/downloader/main.py`

Used by the `bzp-downloader` Cloud Run Job.

### Inputs

| Source | Variable | Default | Description |
|--------|----------|---------|-------------|
| env | `TARGET_DATE` | yesterday | `YYYY-MM-DD`; the date to fetch |
| env | `RUNTIME_ENV` | — | `gcp` in production |
| env | `LAKEHOUSE_BUCKET` | — | GCS bucket name (GCP mode) |
| env | `LOCAL_DATA_ROOT` | — | Local root path (local mode) |

### Behavior

- Fetches BZP API data for `TARGET_DATE` and writes the JSON payload to the
  configured output directory (`bronze_raw/bzp_YYYY-MM-DD.json`).
- Writes a processed manifest at `_processed/fetch/<date>/<hash>.json` on success.
- Returns a non-zero exit code on failure.

---

## Operational Notes

- Both images are built and pushed by CI/CD on every push to `main`.
  Do not build or push manually in normal operation.
- Image tags follow the git short SHA (`GIT_SHA=$(git rev-parse --short HEAD)`).
- No `latest` tag is used in production.
- The Terraform repo owns IAM, Cloud Run job configuration, Artifact Registry,
  and networking. This repo owns the application code and image definitions.
