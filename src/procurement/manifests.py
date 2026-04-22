"""Processed-date manifests for idempotent pipeline steps.

Each pipeline script writes a small JSON marker after successfully completing
a target date.  The backfill DAG reads these markers before submitting a
Dataproc batch and skips the batch when the stored ``script_hash`` matches
the hash of the currently deployed script.

Manifest path (logical, relative to storage root)::

    _processed/{layer}/{target_date}.json

Content::

    {
        "layer":        "bronze",
        "target_date":  "2025-10-01",
        "script_hash":  "<sha256-hex of entry-point .py file>",
        "completed_at": "2025-10-02T03:12:45.123456Z"
    }

Write side (pipeline scripts)::

    from procurement.manifests import write_processed_manifest
    from procurement.obs import sha256_file

    write_processed_manifest(
        layer="bronze",
        target_date=target_date,
        script_hash=sha256_file(Path(__file__)),
        storage=rt.storage,
    )

Read side (Airflow backfill DAG or any skip-check code)::

    from procurement.manifests import is_already_processed

    if is_already_processed("bronze", target_date, current_hash, storage):
        return  # nothing to do

The ``is_already_processed`` check returns ``False`` when the manifest is
absent or when the stored hash does not match ``current_hash`` — both cases
trigger a fresh run.  Pass ``force=True`` at the call site to bypass the
check entirely.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from procurement.runtime.base import StorageProvider


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

_PATH_TMPL = "_processed/{layer}/{target_date}.json"


def _manifest_path(layer: str, target_date: str) -> str:
    return _PATH_TMPL.format(layer=layer, target_date=target_date)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def write_processed_manifest(
    layer: str,
    target_date: str,
    script_hash: str,
    storage: "StorageProvider",
) -> None:
    """Write (or overwrite) the processed manifest for *layer* + *target_date*.

    Idempotent — re-writing on a re-run updates ``completed_at`` and
    ``script_hash`` in place.

    Parameters
    ----------
    layer:
        Pipeline layer name: ``fetch``, ``bronze``, ``silver``, or ``deltas``.
    target_date:
        Processing date in ``YYYY-MM-DD`` format.
    script_hash:
        SHA-256 hex digest of the entry-point script (``sha256_file(Path(__file__))``).
    storage:
        Runtime storage provider — handles both local paths and GCS URIs
        transparently.
    """
    from procurement.obs import now_utc_iso

    storage.write_json(
        _manifest_path(layer, target_date),
        {
            "layer": layer,
            "target_date": target_date,
            "script_hash": script_hash,
            "completed_at": now_utc_iso(),
        },
    )


def is_already_processed(
    layer: str,
    target_date: str,
    current_script_hash: str,
    storage: "StorageProvider",
) -> bool:
    """Return ``True`` if *layer*/*target_date* was processed with *current_script_hash*.

    Returns ``False`` in either of these cases:

    * The manifest does not exist (date was never processed).
    * The manifest exists but ``script_hash`` differs from *current_script_hash*
      (script was updated since the last run).

    Parameters
    ----------
    layer:
        Pipeline layer name.
    target_date:
        Processing date in ``YYYY-MM-DD`` format.
    current_script_hash:
        SHA-256 hex digest of the current entry-point script.
    storage:
        Runtime storage provider.
    """
    data = storage.read_json(_manifest_path(layer, target_date))
    if not data:
        return False
    return data.get("script_hash") == current_script_hash
