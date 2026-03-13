import pandas as pd
import mysql.connector
import logging
import os
import sys
from datetime import datetime

# Add parent directory to path to import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DB_CONFIG, LOG_LEVEL, LOG_FILE

# Setup logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

def log_pipeline_stage(stage, status, message=""):
    """Log pipeline execution stages"""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        cursor.execute(
            "INSERT INTO pipeline_logs (stage, status, message) VALUES (%s, %s, %s)",
            (stage, status, message)
        )
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        logging.warning(f"Failed to log pipeline stage: {str(e)}")

def load_data_to_mysql():
    """Load transformed YouTube data into MySQL database"""
    try:
        logging.info("Starting data loading to MySQL")

        # Check database connection
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Verify table exists
        cursor.execute("SHOW TABLES LIKE 'youtube_videos'")
        if not cursor.fetchone():
            raise Exception("youtube_videos table does not exist. Please run the database schema first.")

        # Ensure directories exist
        processed_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "processed")

        input_path = os.path.join(processed_dir, "youtube_clean.csv")
        if not os.path.exists(input_path):
            raise FileNotFoundError(f"Input file not found: {input_path}")

        df = pd.read_csv(input_path)
        logging.info(f"Loading {len(df)} records to MySQL")

        # Insert data with error handling
        success_count = 0
        error_count = 0

        for index, row in df.iterrows():
            try:
                # Convert datetime format for MySQL
                published_at = str(row["published_at"]).replace('Z', '').replace('T', ' ')

                cursor.execute(
                    """INSERT INTO youtube_videos
                       (video_id, title, channel, published_at, views, likes, comments, category, engagement_rate)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                       ON DUPLICATE KEY UPDATE
                       title=VALUES(title), channel=VALUES(channel), published_at=VALUES(published_at),
                       views=VALUES(views), likes=VALUES(likes), comments=VALUES(comments),
                       category=VALUES(category), engagement_rate=VALUES(engagement_rate)""",
                    (
                        str(row["video_id"]),
                        str(row["title"])[:255],  # Truncate title if too long
                        str(row["channel"])[:255],  # Truncate channel if too long
                        published_at,
                        int(row["views"]),
                        int(row["likes"]),
                        int(row["comments"]),
                        str(row.get("category", "trending")),
                        float(row["engagement_rate"])
                    )
                )
                success_count += 1
            except Exception as e:
                error_count += 1
                logging.warning(f"Failed to insert row {index}: {str(e)}")

        conn.commit()

        # Log pipeline success
        log_pipeline_stage("load", "success", f"Loaded {success_count} records, {error_count} failed")

        logging.info(f"Successfully loaded {success_count} records to MySQL ({error_count} failed)")
        return True

    except mysql.connector.Error as e:
        error_msg = f"MySQL error: {str(e)}"
        logging.error(error_msg)
        log_pipeline_stage("load", "failed", error_msg)
        return False
    except Exception as e:
        error_msg = f"Error in load_data_to_mysql: {str(e)}"
        logging.error(error_msg)
        log_pipeline_stage("load", "failed", error_msg)
        return False
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    success = load_data_to_mysql()
    sys.exit(0 if success else 1)