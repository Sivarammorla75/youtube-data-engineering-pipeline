#!/usr/bin/env python3
"""
YouTube Data Engineering Pipeline Setup Script

This script helps set up the complete environment for the YouTube data pipeline:
1. Install Python dependencies
2. Set up MySQL database
3. Create necessary directories
4. Guide through configuration
"""

import subprocess
import sys
import os
import shutil
from pathlib import Path

def run_command(command, description):
    """Run a shell command and return success status"""
    print(f"\n{description}...")
    try:
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        print("✓ Success")
        return True
    except subprocess.CalledProcessError as e:
        print(f"✗ Failed: {e.stderr}")
        return False

def setup_python_environment():
    """Install Python dependencies"""
    print("\n" + "="*50)
    print("Setting up Python Environment")
    print("="*50)

    # Check if requirements.txt exists
    if not os.path.exists("requirements.txt"):
        print("✗ requirements.txt not found")
        return False

    # Install dependencies
    return run_command("pip install -r requirements.txt", "Installing Python dependencies")

def setup_database():
    """Set up MySQL database"""
    print("\n" + "="*50)
    print("Setting up MySQL Database")
    print("="*50)

    schema_file = "database/schema.sql"
    if not os.path.exists(schema_file):
        print(f"✗ Database schema file not found: {schema_file}")
        return False

    print("Please ensure MySQL is running and you have admin privileges.")
    print("The script will attempt to create the database and tables.")

    # Note: This assumes mysql command is available and user has proper permissions
    # In production, you might want to use a more robust approach
    return run_command(f"mysql -u root -p < {schema_file}", "Creating database schema")

def setup_directories():
    """Create necessary directories"""
    print("\n" + "="*50)
    print("Setting up Directories")
    print("="*50)

    directories = [
        "data/raw",
        "data/processed",
        "data/plots",
        "logs"
    ]

    success = True
    for directory in directories:
        try:
            os.makedirs(directory, exist_ok=True)
            print(f"✓ Created directory: {directory}")
        except Exception as e:
            print(f"✗ Failed to create {directory}: {str(e)}")
            success = False

    return success

def setup_config():
    """Guide user through configuration setup"""
    print("\n" + "="*50)
    print("Setting up Configuration")
    print("="*50)

    config_template = "config_template.py"
    config_file = "config.py"

    if not os.path.exists(config_template):
        print(f"✗ Configuration template not found: {config_template}")
        return False

    if os.path.exists(config_file):
        response = input("config.py already exists. Overwrite? (y/N): ")
        if response.lower() != 'y':
            print("✓ Configuration setup skipped")
            return True

    try:
        shutil.copy(config_template, config_file)
        print("✓ Created config.py from template")
        print("\n" + "!"*50)
        print("IMPORTANT: Please edit config.py and add your credentials:")
        print("1. YouTube API Key: Get from https://console.developers.google.com/")
        print("2. MySQL Password: Your MySQL root password")
        print("!"*50)
        return True
    except Exception as e:
        print(f"✗ Failed to create config file: {str(e)}")
        return False

def main():
    """Main setup function"""
    print("="*60)
    print("YouTube Data Engineering Pipeline Setup")
    print("="*60)

    # Change to script directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)

    steps = [
        ("Setting up directories", setup_directories),
        ("Setting up Python environment", setup_python_environment),
        ("Setting up configuration", setup_config),
        ("Setting up database", setup_database)
    ]

    completed_steps = 0
    total_steps = len(steps)

    for step_name, step_func in steps:
        if step_func():
            completed_steps += 1
        else:
            print(f"\nSetup stopped at: {step_name}")
            break

    print("\n" + "="*60)
    print("SETUP SUMMARY")
    print("="*60)
    print(f"Steps completed: {completed_steps}/{total_steps}")

    if completed_steps == total_steps:
        print("🎉 Setup completed successfully!")
        print("\nNext steps:")
        print("1. Edit config.py with your YouTube API key and MySQL password")
        print("2. Run the pipeline: python run_pipeline.py")
    else:
        print("❌ Setup incomplete. Please resolve the errors above.")

    print("="*60)

if __name__ == "__main__":
    main()