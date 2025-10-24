#!/usr/bin/env python3
"""
Azure App Service startup script for Dash application
This file is required by Azure to properly start the Flask/Dash app
"""
import os
import sys

# Add the IntranetAzure directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'IntranetAzure'))

# Import and run the app
from IntranetAzure.app import app

if __name__ == '__main__':
    # Azure App Service will handle the server startup
    # This file just needs to expose the 'app' variable
    pass
