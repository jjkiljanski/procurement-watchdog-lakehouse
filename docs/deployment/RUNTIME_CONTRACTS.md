# Runtime Contracts (GCP Images)

This repository now exposes three deployable runtime adapters that call the same core scripts used locally:

- `downloader` -> `scripts/pipeline/fetch_bzp_yesterday.py`
- `dispatcher` -> triggers downloader command
- `launcher` -> triggers pipeline command

Core transformation logic remains in `scripts/pipeline/*` and `src/procurement/*`.

## Image Entrypoints

- `Dockerfile.downloader` -> `python apps/downloader/main.py`
- `Dockerfile.dispatcher` -> `python apps/dispatcher/main.py`
- `Dockerfile.launcher` -> `python apps/launcher/main.py`

## Downloader Contract

Job-style adapter.

Inputs:

- `TARGET_DATE` (optional, `YYYY-MM-DD`; default: yesterday)
- `DOWNLOADER_COMMAND_TEMPLATE` (optional; default: `python scripts/pipeline/fetch_bzp_yesterday.py {target_date}`)
- `DOWNLOADER_TIMEOUT_SEC` (optional; default: no timeout)
- `STATE_DONE_DIR` (optional; if set, writes marker `dt=YYYY-MM-DD.done`)

Behavior:

- Executes one command for the resolved day.
- Returns non-zero exit code if command fails.

## Dispatcher Contract

HTTP service with state machine endpoints:

- `POST /backfill/start`
- `POST /tick` (alias: `POST /dispatch`)
- `GET /backfill/status`
- `GET /healthz`

Inputs (env):

- `BACKFILL_ENABLED` (optional; default `true`)
- `BACKFILL_START_DATE` (required if `/backfill/start` body does not provide `start_date`)
- `BACKFILL_END_DATE` (required if `/backfill/start` body does not provide `end_date`)
- `BACKFILL_STATE_PATH` (required; default `data/state/backfill/state.json`)
- `STATE_DONE_DIR` (required; path with `dt=YYYY-MM-DD.done` markers)
- `DISPATCHER_TRIGGER_COMMAND_TEMPLATE` (optional; default: `python apps/downloader/main.py --target-date {target_date}`)
- `DISPATCHER_TRIGGER_TIMEOUT_SEC` (optional; default: `300`)
- `DISPATCHER_FINALIZE_COMMAND_TEMPLATE` (optional; default: `python scripts/ops/run_backfill_finalize.py {target_date}`)
- `DISPATCHER_FINALIZE_TIMEOUT_SEC` (optional; default: `43200`)
- `DISPATCHER_PAUSE_SCHEDULER_COMMAND_TEMPLATE` (optional; no default)
- `DISPATCHER_PAUSE_TIMEOUT_SEC` (optional; default: `120`)
- `DISPATCHER_LOCK_TIMEOUT_SEC` (optional; default: `30`)
- `DISPATCHER_LOCK_STALE_SEC` (optional; default: `600`)

Inputs (request JSON):

- `/backfill/start`: `start_date`, `end_date`, optional `force=true`
- `/tick`: no payload required (current state drives action)

Behavior:

- `/backfill/start` initializes campaign state (`phase=fetching`).
- `/tick`:
  - if pending fetch days exist -> triggers downloader for one day,
  - else -> runs finalize command once,
  - after successful finalize -> marks `phase=done` and optionally pauses scheduler.
- `/backfill/status` returns persisted state JSON.

## Launcher Contract

HTTP service (`POST /launch`).

Inputs (env):

- `LAUNCHER_COMMAND_TEMPLATE` (optional; default: `python scripts/ops/run_transforms_for_day.py {target_date}`)
- `LAUNCHER_TIMEOUT_SEC` (optional; default: `7200`)
- `TARGET_DATE` (optional fallback)

Inputs (request JSON):

- `target_date` (optional explicit date)

Behavior:

- Executes launch command for the chosen date.
- Returns command status in JSON.

## Operational Notes

- These adapters are thin runtime wrappers only.
- IaC repository should own IAM, Scheduler, Cloud Run deployment, secrets, and network policy.
- Keep image tags pinned (no `latest`) in production.
