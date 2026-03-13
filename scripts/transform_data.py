import pandas as pd
import logging
import os
import sys
from datetime import datetime

# Add parent directory to path to import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import LOG_LEVEL, LOG_FILE

# Setup logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

def transform_data():
    """Transform raw YouTube data by cleaning and adding derived metrics"""
    try:
        logging.info("Starting data transformation")

        # Ensure directories exist
        raw_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "raw")
        processed_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "processed")
        os.makedirs(processed_dir, exist_ok=True)

        input_path = os.path.join(raw_dir, "youtube_raw.csv")
        output_path = os.path.join(processed_dir, "youtube_clean.csv")

        if not os.path.exists(input_path):
            raise FileNotFoundError(f"Input file not found: {input_path}")

        df = pd.read_csv(input_path)

        # Data validation and cleaning
        logging.info(f"Loaded {len(df)} records from raw data")

        # Convert numeric columns and handle missing values
        numeric_columns = ['views', 'likes', 'comments']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

        # Add derived metrics
        df['engagement_rate'] = df.apply(
            lambda row: (row['likes'] + row['comments']) / row['views'] if row['views'] > 0 else 0,
            axis=1
        )

        # Add data quality metrics
        df['has_views'] = df['views'] > 0
        df['has_likes'] = df['likes'] > 0

        # Remove duplicates based on video_id if available
        if 'video_id' in df.columns:
            initial_count = len(df)
            df = df.drop_duplicates(subset=['video_id'])
            logging.info(f"Removed {initial_count - len(df)} duplicate records")

        # Ensure processed directory exists
        os.makedirs("../data/processed", exist_ok=True)

        df.to_csv(output_path, index=False)

        logging.info(f"Successfully transformed data. Output: {len(df)} records saved to {output_path}")
        logging.info(f"Data quality summary: {df['has_views'].sum()}/{len(df)} videos have views, {df['has_likes'].sum()}/{len(df)} have likes")

        return True

    except Exception as e:
        logging.error(f"Error in transform_data: {str(e)}")
        return False

if __name__ == "__main__":
    success = transform_data()
    sys.exit(0 if success else 1)