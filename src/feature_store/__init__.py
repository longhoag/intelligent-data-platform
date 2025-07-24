"""
Feature Store Infrastructure - Day 4 Implementation
Production-ready feature store with online/offline serving and versioning
"""

try:
    from .store import FeatureStore
    from .registry import FeatureRegistry
    from .cache import FeatureCache
    store_available = True
except ImportError as e:
    print(f"Warning: Core feature store unavailable due to missing dependencies: {e}")
    store_available = False

try:
    from .server import FeatureServer
    server_available = True
except ImportError as e:
    print(f"Warning: Feature server unavailable due to missing dependencies: {e}")
    server_available = False

__all__ = []
if store_available:
    __all__.extend(["FeatureStore", "FeatureRegistry", "FeatureCache"])
if server_available:
    __all__.append("FeatureServer")
