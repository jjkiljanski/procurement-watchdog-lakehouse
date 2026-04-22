"""Runtime abstraction layer for the procurement pipeline.

Import surface::

    from procurement.runtime import get_runtime, RuntimeConfig

See :mod:`procurement.runtime.base` for the abstract interfaces and
:mod:`procurement.runtime.config` for the factory function.
"""

from procurement.runtime.base import RuntimeConfig
from procurement.runtime.config import get_runtime

__all__ = ["get_runtime", "RuntimeConfig"]
