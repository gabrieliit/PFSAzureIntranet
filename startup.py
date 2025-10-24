#!/usr/bin/env python3
"""
Azure App Service startup script for Dash application
This file is required by Azure to properly start the Flask/Dash app
"""
import os
import sys

# Add the IntranetAzure directory to Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
intranet_dir = os.path.join(current_dir, 'IntranetAzure')

# Debug: print paths for troubleshooting
print(f"Current directory: {current_dir}")
print(f"IntranetAzure directory: {intranet_dir}")
print(f"Python path: {sys.path}")

# Add IntranetAzure to Python path
if intranet_dir not in sys.path:
    sys.path.insert(0, intranet_dir)

# Import and run the app
try:
    print("Attempting to import from IntranetAzure.app...")
    from IntranetAzure.app import app
    print("Successfully imported app from IntranetAzure.app")
except ImportError as e:
    print(f"Import error: {e}")
    # Fallback: try importing directly
    sys.path.insert(0, current_dir)
    try:
        from IntranetAzure.app import app
        print("Successfully imported app with fallback path")
    except ImportError as e2:
        print(f"Fallback import error: {e2}")
        raise

if __name__ == '__main__':
    print("Startup script completed successfully")
    # Azure App Service will handle the server startup
    # This file just needs to expose the 'app' variable
    pass
