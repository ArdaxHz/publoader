#!/bin/bash

echo "Installing Python dependencies from requirements.txt files..."

# Recursively install all requirements.txt files in the extensions directory
find /app -name "requirements.txt" -exec pip install -r {} \;

echo "Python dependencies installed."

# Execute the main command passed to the container
exec python run.py "$@"
