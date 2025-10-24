#!/usr/bin/env python3
"""
Simple Azure startup script - direct import approach
"""
import sys
import os

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import the Flask app directly
from IntranetAzure.app import app

if __name__ == '__main__':
    pass
