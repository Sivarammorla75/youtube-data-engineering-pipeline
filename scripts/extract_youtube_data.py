import requests
import pandas as pd
import logging
import os
import sys
from datetime import datetime

# Add parent directory to path to import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import YOUTUBE_API_KEY, MAX_RESULTS, REGION_CODE, CHART_TYPE, LOG_LEVEL, LOG_FILE

# Setup logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

def extract_youtube_data():
    """Extract YouTube data using the YouTube Data API v3"""
    try:
        logging.info("Starting YouTube data extraction")

        if not YOUTUBE_API_KEY or YOUTUBE_API_KEY == "YOUR_YOUTUBE_API_KEY_HERE":
            raise ValueError("YouTube API key not configured. Please update config.py")

        url = f"https://www.googleapis.com/youtube/v3/videos?part=snippet,statistics&chart={CHART_TYPE}&regionCode={REGION_CODE}&maxResults={MAX_RESULTS}&key={YOUTUBE_API_KEY}"

        logging.info(f"Making API request to: {url}")
        response = requests.get(url)

        if response.status_code != 200:
            raise Exception(f"API request failed with status code {response.status_code}: {response.text}")

        data = response.json()

        if "error" in data:
            raise Exception(f"YouTube API error: {data['error']['message']}")

        videos = []

        for item in data.get("items", []):
            video_data = {
                "video_id": item["id"],
                "title": item["snippet"]["title"],
                "channel": item["snippet"]["channelTitle"],
                "published_at": item["snippet"]["publishedAt"],
                "views": int(item["statistics"].get("viewCount", 0)),
                "likes": int(item["statistics"].get("likeCount", 0)),
                "comments": int(item["statistics"].get("commentCount", 0)),
                "extracted_at": datetime.now().isoformat()
            }
            videos.append(video_data)

        df = pd.DataFrame(videos)

        # Ensure data/raw directory exists
        raw_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "raw")
        os.makedirs(raw_dir, exist_ok=True)

        output_path = os.path.join(raw_dir, "youtube_raw.csv")
        df.to_csv(output_path, index=False)

        logging.info(f"Successfully extracted {len(videos)} videos to {output_path}")
        return True

    except Exception as e:
        logging.error(f"Error in extract_youtube_data: {str(e)}")
        return False

if __name__ == "__main__":
    success = extract_youtube_data()
    sys.exit(0 if success else 1)