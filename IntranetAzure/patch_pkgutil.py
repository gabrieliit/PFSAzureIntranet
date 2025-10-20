"""
Monkey patch to fix pkgutil.find_loader compatibility issue with Python 3.14
This patch replaces pkgutil.find_loader with importlib.util.find_spec
"""
import importlib.util
import pkgutil

# Store original find_loader if it exists
_original_find_loader = getattr(pkgutil, 'find_loader', None)

def patched_find_loader(name):
    """Replacement for pkgutil.find_loader using importlib.util.find_spec"""
    try:
        spec = importlib.util.find_spec(name)
        if spec is not None and spec.loader is not None:
            return spec.loader
        return None
    except (ImportError, AttributeError, ValueError):
        return None

# Apply the patch
if not hasattr(pkgutil, 'find_loader'):
    pkgutil.find_loader = patched_find_loader
    print("Applied pkgutil.find_loader patch for Python 3.14 compatibility")
