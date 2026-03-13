#!/usr/bin/env python3
"""
YouTube Data Engineering Pipeline
End-to-End ETL Pipeline for YouTube Data Analysis

This script orchestrates the complete data pipeline:
1. Extract: Fetch data from YouTube API
2. Transform: Clean and process the data
3. Load: Store data in MySQL database
4. Analyze: Generate insights and visualizations
"""

import subprocess
import sys
import os
import logging
import time
from datetime import datetime

# Add current directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def setup_logging():
    """Setup logging configuration"""
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(f"{log_dir}/pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
            logging.StreamHandler(sys.stdout)
        ]
    )

def run_stage(script_name, stage_name):
    """Run a pipeline stage script and return success status"""
    try:
        logging.info(f"Starting {stage_name} stage...")

        script_path = f"scripts/{script_name}"
        if not os.path.exists(script_path):
            raise FileNotFoundError(f"Script not found: {script_path}")

        start_time = time.time()
        result = subprocess.run([sys.executable, script_path],
                              capture_output=True, text=True, cwd=os.path.dirname(__file__))

        end_time = time.time()
        duration = end_time - start_time

        if result.returncode == 0:
            logging.info(f"✓ {stage_name} completed successfully in {duration:.2f} seconds")
            if result.stdout:
                logging.info(f"Output: {result.stdout.strip()}")
            return True
        else:
            logging.error(f"✗ {stage_name} failed with return code {result.returncode}")
            if result.stderr:
                logging.error(f"Error: {result.stderr.strip()}")
            return False

    except Exception as e:
        logging.error(f"✗ {stage_name} failed with exception: {str(e)}")
        return False

def check_config():
    """Check if configuration is properly set up"""
    try:
        from config import YOUTUBE_API_KEY, DB_CONFIG

        if not YOUTUBE_API_KEY or YOUTUBE_API_KEY == "YOUR_YOUTUBE_API_KEY_HERE":
            logging.error("YouTube API key not configured. Please update config.py")
            return False

        if DB_CONFIG["password"] == "YOUR_MYSQL_PASSWORD_HERE":
            logging.error("MySQL password not configured. Please update config.py")
            return False

        logging.info("Configuration check passed")
        return True

    except ImportError:
        logging.error("config.py not found. Please copy config_template.py to config.py and configure it")
        return False
    except Exception as e:
        logging.error(f"Configuration error: {str(e)}")
        return False

def main():
    """Main pipeline execution function"""
    print("="*60)
    print("YouTube Data Engineering Pipeline")
    print("="*60)

    setup_logging()
    logging.info("Pipeline execution started")

    # Check configuration
    if not check_config():
        logging.error("Pipeline aborted due to configuration issues")
        sys.exit(1)

    # Define pipeline stages
    stages = [
        ("extract_youtube_data.py", "Data Extraction"),
        ("transform_data.py", "Data Transformation"),
        ("load_to_mysql.py", "Data Loading"),
        ("analyze_data.py", "Data Analysis")
    ]

    # Execute pipeline stages
    success_count = 0
    total_stages = len(stages)

    for script, stage_name in stages:
        if run_stage(script, stage_name):
            success_count += 1
        else:
            logging.error(f"Pipeline stopped at {stage_name} stage")
            break

    # Summary
    print("\n" + "="*60)
    print("PIPELINE EXECUTION SUMMARY")
    print("="*60)
    print(f"Stages completed: {success_count}/{total_stages}")

    if success_count == total_stages:
        logging.info("🎉 Pipeline completed successfully!")
        print("✓ All stages completed successfully")
        print("📊 Check data/plots/ for visualizations")
        print("📝 Check data/analysis_summary.txt for detailed statistics")
    else:
        logging.error("❌ Pipeline failed or partially completed")
        print("✗ Pipeline execution incomplete")

    print("="*60)

if __name__ == "__main__":
    main()