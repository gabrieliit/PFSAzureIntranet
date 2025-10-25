"""
Compatibility module for dash_table import
This module provides backward compatibility for dash_table imports
"""
try:
    # Try to import from the deprecated dash_table package
    import dash_table
    print("Using deprecated dash_table package")
except ImportError:
    try:
        # Fallback: import from dash package
        from dash import dash_table
        print("Using dash_table from main dash package")
    except ImportError:
        # If both fail, create a mock module
        print("Creating mock dash_table module")
        import sys
        from types import ModuleType
        
        dash_table = ModuleType('dash_table')
        dash_table.DataTable = None  # This will cause an error if used, but allows import
        sys.modules['dash_table'] = dash_table
