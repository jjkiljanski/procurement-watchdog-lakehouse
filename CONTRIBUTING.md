# Contributing

This repository is organized to separate production pipeline code from operations wrappers and exploratory tooling.

## Repository layout

- `src/procurement/`:
  Production library code (Bronze/Silver transforms, parsers, shared utilities).
- `scripts/pipeline/`:
  Production entrypoints for data jobs (`build_*`, `fetch_*`).
- `scripts/ops/`:
  Operational wrappers and orchestration scripts (`run_*`, `backfill_*`, chain/watch helpers).
- `scripts/dev/`:
  Exploratory/one-off tools. Non-production.
- `docs/data_model/`:
  Data model and profiling docs.
- `docs/legal/`:
  Legal/domain interpretation docs.
- `docs/deployment/`:
  Environment and deployment requirements.
- `docs/runbooks/`:
  Operational procedures (daily/backfill/recovery/concurrency).

## Naming conventions

- Keep `build_*` names for pipeline build jobs.
- Keep `fetch_*` names for data ingestion/fetch jobs.
- Keep `run_*` names for orchestration wrappers.
- Avoid adding exploratory scripts to `scripts/pipeline/`.

## Commit hygiene

- Keep commits scoped to one logical change.
- Do not mix repository cleanup/reorg with functional pipeline changes in one commit.
- Prefer committing code + tests together.

## Generated artifacts policy

Do not commit runtime/generated artifacts:

- `logs/`, `*.out.log`, `*.err.log`
- generated HTML report assets (`*_files/`, rendered `.html` unless explicitly required)
- temporary lock/debug files

Keep generated data outside the repository tree (or under ignored paths).

## Testing

Run targeted tests for changed components first, then broader test suites when possible:

```bash
pytest tests/silver/ -q
pytest -q
```

## Documentation updates

When moving scripts or changing operational flow:

1. Update `README.md`.
2. Update `docs/README.md`.
3. Update relevant runbooks in `docs/runbooks/`.
