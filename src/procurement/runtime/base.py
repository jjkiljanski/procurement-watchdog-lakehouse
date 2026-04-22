"""Abstract interfaces for the procurement pipeline runtime.

Every provider (local, GCP, future Azure/AWS) must implement these three
classes.  Pipeline scripts import only from this module so that swapping a
provider requires no changes to business logic.

Design principles
-----------------
- ``StorageProvider`` owns *where* data lives (path resolution) and simple
  file operations (JSON state, existence checks, GCS-style locks).
- ``SparkLauncher`` owns *how* Spark jobs run (local session vs. Dataproc
  Serverless batch submission) and injects provider-specific session config
  (GCS connector, Iceberg catalog, etc.).
- ``StateBackend`` owns lightweight key/value state used by the backfill DAG
  (not the heavy Parquet data).

Adding a new provider
---------------------
1. Create ``src/procurement/runtime/providers/<name>.py``
2. Implement ``StorageProvider``, ``SparkLauncher``, ``StateBackend``
3. Add an ``elif RUNTIME_ENV == "<name>"`` branch in
   ``src/procurement/runtime/config.py``
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Generator

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


# ---------------------------------------------------------------------------
# StorageProvider
# ---------------------------------------------------------------------------


class StorageProvider(ABC):
    """Resolves logical data paths and performs lightweight storage ops."""

    @abstractmethod
    def resolve(self, logical_path: str) -> str:
        """Return the physical URI or absolute path for a logical path.

        Examples::

            # local
            provider.resolve("bronze")        → "/abs/data/bronze"
            provider.resolve("bronze_raw")    → "/abs/data/bronze_raw"

            # GCP
            provider.resolve("bronze")        → "gs://my-bucket/bronze"
        """

    @abstractmethod
    def exists(self, logical_path: str) -> bool:
        """Return True if the resolved path exists (object or directory)."""

    @abstractmethod
    def read_json(self, logical_path: str) -> dict[str, Any]:
        """Read and deserialise a JSON file at the resolved path."""

    @abstractmethod
    def write_json(self, logical_path: str, data: dict[str, Any]) -> None:
        """Serialise *data* and write atomically to the resolved path."""

    @abstractmethod
    def list_prefixes(self, logical_path: str) -> list[str]:
        """Return immediate child prefix names (partition directory names)."""

    @contextmanager
    def acquire_lock(self, key: str) -> Generator[None, None, None]:
        """Context manager that acquires an advisory lock identified by *key*.

        The default implementation is a no-op (safe in Airflow where tasks are
        already serialised).  Local and GCP providers override this when
        concurrent writes to the same partition are possible.
        """
        yield

    def obs_path(self) -> Path | None:
        """Return the local Path for observability Parquet writes, or None.

        ``None`` means obs writing is not supported in this provider (e.g. GCP
        Dataproc workers have no persistent local storage).  Pipeline scripts
        must skip obs writes when this returns None.

        TODO: extend obs.py to write directly to GCS so this can return a
        non-None value for the GCP provider.
        """
        return None


# ---------------------------------------------------------------------------
# SparkLauncher
# ---------------------------------------------------------------------------


class SparkLauncher(ABC):
    """Creates SparkSessions and submits batch jobs."""

    @abstractmethod
    def get_session(self, app_name: str, **extra_config: str) -> "SparkSession":
        """Return a configured, ready-to-use SparkSession.

        The provider is responsible for injecting connector jars, catalog
        config, GCS credentials, etc.  Callers must call ``spark.stop()``
        when finished.
        """

    @abstractmethod
    def submit_batch(
        self,
        script_path: str,
        args: list[str],
        job_id: str,
        *,
        wait: bool = True,
    ) -> int:
        """Submit a PySpark script as a batch job.

        Parameters
        ----------
        script_path:
            Path or GCS URI to the PySpark entry-point script.
        args:
            Command-line arguments forwarded to the script.
        job_id:
            Human-readable identifier used for logging / Dataproc batch name.
        wait:
            If True, block until the job completes and return its exit code.
            If False, fire-and-forget and return 0.

        Returns
        -------
        int
            Exit code (0 = success).
        """


# ---------------------------------------------------------------------------
# StateBackend
# ---------------------------------------------------------------------------


class StateBackend(ABC):
    """Lightweight key/value store for pipeline state (backfill progress etc.)."""

    @abstractmethod
    def load(self, state_key: str) -> dict[str, Any]:
        """Load state dict for *state_key*.  Returns ``{}`` if not found."""

    @abstractmethod
    def save(self, state_key: str, data: dict[str, Any]) -> None:
        """Persist *data* under *state_key*, overwriting any existing value."""


# ---------------------------------------------------------------------------
# RuntimeConfig
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RuntimeConfig:
    """Aggregates the three provider instances for a given runtime environment.

    Obtain via :func:`procurement.runtime.config.get_runtime`.

    Attributes
    ----------
    env:
        The resolved ``RUNTIME_ENV`` value (``"local"`` or ``"gcp"``).
    storage:
        Resolves paths and handles file I/O.
    spark:
        Creates SparkSessions and submits batch jobs.
    state:
        Stores lightweight pipeline state (e.g. backfill progress).
    """

    env: str
    storage: StorageProvider
    spark: SparkLauncher
    state: StateBackend
