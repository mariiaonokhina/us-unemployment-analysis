#!/bin/bash

echo "===== SETTING UP PYTHON ENVIRONMENT ====="

# Create venv
python3 -m venv venv
source venv/bin/activate

# Install Python dependencies
pip install -r requirements/python_requirements.txt
echo "SUCCESS: Python libraries installed"

echo "===== CHECKING SYSTEM REQUIREMENTS ====="

# Install command-line tools if missing
command -v wget >/dev/null 2>&1 || brew install wget
command -v curl >/dev/null 2>&1 || brew install curl
command -v unzip >/dev/null 2>&1 || brew install unzip

# Install Java if needed
if ! command -v java >/dev/null 2>&1; then
    brew install openjdk@17
    export JAVA_HOME=$(/usr/libexec/java_home)
fi

# Install Spark if missing
command -v spark-shell >/dev/null 2>&1 || brew install apache-spark

echo "SETUP COMPLETE"
