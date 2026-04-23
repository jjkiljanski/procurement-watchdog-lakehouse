"""Local filesystem provider.

Used when ``RUNTIME_ENV=local`` (the default).  All data lives under
``LOCAL_DATA_ROOT`` (default: ``data/``), SparkSession runs in local mode,
and state is stored as JSON files on disk.

This provider wraps the existing behaviour of the pipeline scripts — switching
from local to GCP requires only setting ``RUNTIME_ENV=gcp`` plus the GCP
variables listed in ``config/runtime_gcp.env.example``.

Environment variables
---------------------
``LOCAL_DATA_ROOT``
    Root directory for all resolved paths.  Relative paths are resolved
    relative to the current working directory.  Default: ``data``.
``SPARK_MASTER``
    Spark master string.  Default: ``local[*]``.
``SPARK_APP_EXTRA_CONFIG``
    Optional JSON object of extra ``spark.conf`` key/value pairs.
"""

from __future__ import annotations

import json
import os
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Generator

from procurement.runtime.base import RuntimeConfig, SparkLauncher, StateBackend, StorageProvider


# ---------------------------------------------------------------------------
# LocalStorageProvider
# ---------------------------------------------------------------------------


class LocalStorageProvider(StorageProvider):
    """Resolves logical paths to absolute local filesystem paths."""

    def __init__(self, data_root: Path) -> None:
        self._root = data_root.resolve()

    @property
    def root(self) -> Path:
        return self._root

    # ------------------------------------------------------------------
    # Core interface
    # ------------------------------------------------------------------

    def resolve(self, logical_path: str) -> str:
        """Return the absolute filesystem path for *logical_path*."""
        return str(self._root / logical_path)

    def exists(self, logical_path: str) -> bool:
        return Path(self.resolve(logical_path)).exists()

    def read_json(self, logical_path: str) -> dict[str, Any]:
        path = Path(self.resolve(logical_path))
        if not path.exists():
            return {}
        return json.loads(path.read_text(encoding="utf-8"))

    def write_json(self, logical_path: str, data: dict[str, Any]) -> None:
        """Write *data* atomically (write .tmp then rename)."""
        path = Path(self.resolve(logical_path))
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(path)

    def list_prefixes(self, logical_path: str) -> list[str]:
        """Return immediate child directory names."""
        target = Path(self.resolve(logical_path))
        if not target.is_dir():
            return []
        return [p.name for p in sorted(target.iterdir()) if p.is_dir()]

    @contextmanager
    def acquire_lock(self, key: str) -> Generator[None, None, None]:
        """Acquire a lightweight directory lock; release on exit."""
        import time

        lock_dir = self._root / "_locks" / key
        lock_dir.parent.mkdir(parents=True, exist_ok=True)

        # Simple spin-lock: try mkdir (atomic on POSIX + Windows NTFS).
        deadline = time.time() + 300  # 5-minute timeout
        while True:
            try:
                lock_dir.mkdir(parents=False, exist_ok=False)
                break
            except FileExistsError:
                if time.time() > deadline:
                    raise TimeoutError(f"Could not acquire lock {lock_dir} within 300 s")
                time.sleep(2)
        try:
            yield
        finally:
            import shutil
            shutil.rmtree(lock_dir, ignore_errors=True)

    def obs_path(self) -> Path:
        return self._root / "obs"


# ---------------------------------------------------------------------------
# LocalSparkLauncher
# ---------------------------------------------------------------------------


class LocalSparkLauncher(SparkLauncher):
    """Creates local SparkSessions and runs batches as subprocesses."""

    def __init__(self, master: str, data_root: Path, extra_config: dict[str, str] | None = None) -> None:
        self._master = master
        self._data_root = data_root.resolve()
        self._extra_config = extra_config or {}

    def get_session(self, app_name: str, **extra_config: str):
        from pyspark.sql import SparkSession

        warehouse = str(self._data_root / "iceberg")

        # If SPARK_EXTRA_CLASSPATH is set (e.g. pointing to the Iceberg JAR
        # inside the Docker container), add it to the driver classpath so
        # the Iceberg extensions and catalog classes are available.
        extra_classpath = os.environ.get("SPARK_EXTRA_CLASSPATH", "")

        builder = (
            SparkSession.builder.appName(app_name)
            .master(self._master)
            .config("spark.pyspark.python", sys.executable)
            .config("spark.pyspark.driver.python", sys.executable)
            .config("spark.sql.ansi.enabled", "false")
            .config("spark.sql.mapKeyDedupPolicy", "LAST_WIN")
            .config("spark.scheduler.mode", "FAIR")
            .config("spark.sql.parquet.columnarReaderBatchSize", "1024")
            .config(
                "spark.driver.extraJavaOptions",
                "-XX:+UseG1GC -XX:G1HeapRegionSize=4m -XX:+ExplicitGCInvokesConcurrent",
            )
            # Iceberg extensions — silver writes use Iceberg tables on the
            # HadoopCatalog rooted at data/iceberg/.  See docs/iceberg.md.
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .config("spark.sql.catalog.silver", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.silver.type", "hadoop")
            .config("spark.sql.catalog.silver.warehouse", warehouse)
        )
        if extra_classpath:
            builder = builder.config("spark.driver.extraClassPath", extra_classpath)
        for k, v in {**self._extra_config, **extra_config}.items():
            builder = builder.config(k, v)
        return builder.getOrCreate()

    def submit_batch(
        self,
        script_path: str,
        args: list[str],
        job_id: str,
        *,
        wait: bool = True,
    ) -> int:
        """Run the script as a subprocess (mirrors Dataproc Serverless API)."""
        import subprocess

        cmd = [sys.executable, script_path, *args]
        if not wait:
            subprocess.Popen(cmd)
            return 0
        result = subprocess.run(cmd, check=False)
        return result.returncode


# ---------------------------------------------------------------------------
# LocalStateBackend
# ---------------------------------------------------------------------------


class LocalStateBackend(StateBackend):
    """Stores pipeline state as JSON files under ``{data_root}/_state/``."""

    def __init__(self, state_dir: Path) -> None:
        self._dir = state_dir

    def load(self, state_key: str) -> dict[str, Any]:
        path = self._dir / f"{state_key}.json"
        if not path.exists():
            return {}
        return json.loads(path.read_text(encoding="utf-8"))

    def save(self, state_key: str, data: dict[str, Any]) -> None:
        self._dir.mkdir(parents=True, exist_ok=True)
        path = self._dir / f"{state_key}.json"
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(path)


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def build_local_runtime() -> RuntimeConfig:
    """Construct a :class:`RuntimeConfig` for the local filesystem."""
    data_root_env = os.environ.get("LOCAL_DATA_ROOT", "data")
    data_root = Path(data_root_env)

    master = os.environ.get("SPARK_MASTER", "local[*]")

    extra_config_raw = os.environ.get("SPARK_APP_EXTRA_CONFIG", "")
    extra_config: dict[str, str] = {}
    if extra_config_raw.strip():
        try:
            extra_config = json.loads(extra_config_raw)
        except json.JSONDecodeError as exc:
            raise ValueError(
                f"SPARK_APP_EXTRA_CONFIG is not valid JSON: {exc}"
            ) from exc

    storage = LocalStorageProvider(data_root)
    spark = LocalSparkLauncher(master=master, data_root=data_root, extra_config=extra_config)
    state = LocalStateBackend(data_root / "_state")

    from procurement.runtime.base import RuntimeConfig

    return RuntimeConfig(env="local", storage=storage, spark=spark, state=state)
