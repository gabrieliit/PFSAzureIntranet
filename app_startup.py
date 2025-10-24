#!/usr/bin/env python3
"""
Azure startup script - handles both directory structures
"""
import sys
import os

# Debug: Print current working directory and file structure
print(f"Current working directory: {os.getcwd()}")
print(f"Script location: {os.path.dirname(os.path.abspath(__file__))}")
print(f"Files in current directory: {os.listdir('.')}")

# Add current directory to path
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

# Check if IntranetAzure directory exists
intranet_dir = os.path.join(current_dir, 'IntranetAzure')
if os.path.exists(intranet_dir):
    print(f"IntranetAzure directory found at: {intranet_dir}")
    sys.path.insert(0, intranet_dir)
    print(f"Files in IntranetAzure: {os.listdir(intranet_dir)}")
    # Try importing from IntranetAzure
    try:
        from IntranetAzure.app import app
        print("Successfully imported app from IntranetAzure.app")
    except ImportError as e:
        print(f"Failed to import from IntranetAzure.app: {e}")
        raise
else:
    print("IntranetAzure directory not found, checking if files are in root...")
    # Check if app.py is in the root directory
    if os.path.exists('app.py'):
        print("app.py found in root directory")
        try:
            from app import app
            print("Successfully imported app from root app")
        except ImportError as e:
            print(f"Failed to import from root app: {e}")
            raise
    else:
        print("app.py not found in root directory")
        raise ImportError("Cannot find app.py or IntranetAzure directory")

if __name__ == '__main__':
    print("Startup script completed successfully")
    pass
