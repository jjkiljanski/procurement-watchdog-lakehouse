"""Backward-compat shim: re-exports from obs.py.

All canonical implementations live in procurement.obs.
"""

from procurement.obs import atomic_write_json, git_commit_sha, now_utc_iso, sha256_file


def script_hashes(paths: list) -> dict:
    """Return {path: sha256} for existing paths. Kept for old callers."""
    return {str(p): sha256_file(p) for p in paths if p.exists()}


__all__ = [
    "atomic_write_json",
    "git_commit_sha",
    "now_utc_iso",
    "script_hashes",
    "sha256_file",
]
