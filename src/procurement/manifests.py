"""Processed-date manifests for idempotent pipeline steps.

Each pipeline script writes a small JSON marker after successfully completing
a target date.  The backfill workflow reads these markers before submitting a
Dataproc batch and skips the batch when the stored ``script_hash`` matches
the hash of the currently deployed script.

Manifest paths (logical, relative to storage root)
---------------------------------------------------
Bronze / deltas  (per-day, notice-type-agnostic)::

    _processed/{layer}/{target_date}.json

Silver  (per-notice-type, per-day — written inside run_silver_*_core() after
each notice-type batch succeeds)::

    _processed/silver/{target_date}/{notice_type}.json

The ``notice_type`` parameter controls which path is used.  Passing
``notice_type=None`` (the default) uses the legacy per-day path, which is
still correct for bronze and deltas.

Manifest content::

    {
        "layer":        "silver",
        "target_date":  "2025-10-01",
        "notice_type":  "ContractNotice",   # null for bronze/deltas
        "script_hash":  "<sha256-hex of entry-point .py file>",
        "dependency_hashes": {
            "bronze": "<sha256-hex of upstream stage code>"
        },
        "completed_at": "2025-10-02T03:12:45.123456Z"
    }

Write side::

    from procurement.manifests import write_processed_manifest
    from procurement.obs import sha256_file

    # Bronze / deltas — per-day
    write_processed_manifest(
        layer="bronze",
        target_date=target_date,
        script_hash=sha256_file(Path(__file__)),
        storage=rt.storage,
    )

    # Silver — per (date, notice_type)
    write_processed_manifest(
        layer="silver",
        target_date=target_date,
        script_hash=sha256_file(Path(__file__)),
        storage=rt.storage,
        notice_type="ContractNotice",
    )

Read side::

    from procurement.manifests import is_already_processed

    if is_already_processed("bronze", target_date, current_hash, storage):
        return  # skip bronze for this day

    if is_already_processed("silver", target_date, current_hash, storage,
                            notice_type="ContractNotice"):
        return  # skip this (date, notice_type) pair

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
_PATH_NT_TMPL = "_processed/{layer}/{target_date}/{notice_type}.json"


def _manifest_path(layer: str, target_date: str, notice_type: str | None = None) -> str:
    if notice_type is not None:
        return _PATH_NT_TMPL.format(layer=layer, target_date=target_date, notice_type=notice_type)
    return _PATH_TMPL.format(layer=layer, target_date=target_date)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def write_processed_manifest(
    layer: str,
    target_date: str,
    script_hash: str,
    storage: "StorageProvider",
    notice_type: str | None = None,
    dependency_hashes: dict[str, str] | None = None,
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
    notice_type:
        When set, write a per-(date, notice_type) manifest under
        ``_processed/{layer}/{date}/{notice_type}.json``.  Used by silver
        processing so individual (date, notice_type) pairs can be skipped on
        re-runs without reprocessing the whole day.  ``None`` (default) writes
        the legacy per-day path used by bronze and deltas.
    dependency_hashes:
        Optional mapping of upstream stage names to the code hash that produced
        this output.  A later ``is_already_processed`` call with a different
        mapping returns ``False`` so downstream outputs are rebuilt after
        upstream code changes.
    """
    from procurement.obs import now_utc_iso

    storage.write_json(
        _manifest_path(layer, target_date, notice_type),
        {
            "layer": layer,
            "target_date": target_date,
            "notice_type": notice_type,
            "script_hash": script_hash,
            "dependency_hashes": dependency_hashes or {},
            "completed_at": now_utc_iso(),
        },
    )


def is_already_processed(
    layer: str,
    target_date: str,
    current_script_hash: str,
    storage: "StorageProvider",
    notice_type: str | None = None,
    dependency_hashes: dict[str, str] | None = None,
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
    notice_type:
        When set, checks the per-(date, notice_type) manifest path.  Must match
        the value used when ``write_processed_manifest`` was called.
    dependency_hashes:
        Optional upstream stage hashes that must exactly match the manifest's
        stored ``dependency_hashes`` mapping.
    """
    data = storage.read_json(_manifest_path(layer, target_date, notice_type))
    if not data:
        return False
    if data.get("script_hash") != current_script_hash:
        return False
    if dependency_hashes is not None:
        return data.get("dependency_hashes", {}) == dependency_hashes
    return True


def all_notice_types_processed(
    target_date: str,
    current_script_hash: str,
    notice_types: "list[str]",
    storage: "StorageProvider",
    dependency_hashes: dict[str, str] | None = None,
) -> bool:
    """Return ``True`` only when every notice type in *notice_types* has a
    matching silver manifest for *target_date*.

    Used by the backfill workflow to skip a date that was fully processed in a
    prior run.
    """
    return all(
        is_already_processed(
            "silver",
            target_date,
            current_script_hash,
            storage,
            nt,
            dependency_hashes=dependency_hashes,
        )
        for nt in notice_types
    )
