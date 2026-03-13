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

        videos = []

        # Strategy 1: Get trending videos (up to 50)
        logging.info("Fetching trending videos...")
        trending_url = f"https://www.googleapis.com/youtube/v3/videos?part=snippet,statistics&chart={CHART_TYPE}&regionCode={REGION_CODE}&maxResults=50&key={YOUTUBE_API_KEY}"

        response = requests.get(trending_url)
        if response.status_code == 200:
            data = response.json()
            if "items" in data:
                for item in data["items"]:
                    video_data = {
                        "video_id": item["id"],
                        "title": item["snippet"]["title"],
                        "channel": item["snippet"]["channelTitle"],
                        "published_at": item["snippet"]["publishedAt"],
                        "views": int(item["statistics"].get("viewCount", 0)),
                        "likes": int(item["statistics"].get("likeCount", 0)),
                        "comments": int(item["statistics"].get("commentCount", 0)),
                        "category": "trending",
                        "extracted_at": datetime.now().isoformat()
                    }
                    videos.append(video_data)
                logging.info(f"Fetched {len(data['items'])} trending videos")

        # Strategy 2: Search for movie-related content
        movie_queries = ["movie trailer", "film review", "cinema", "hollywood", "movie scene"]
        videos_per_query = 20

        for query in movie_queries:
            logging.info(f"Searching for: {query}")
            search_url = f"https://www.googleapis.com/youtube/v3/search?part=snippet&q={query}&type=video&order=relevance&maxResults={videos_per_query}&key={YOUTUBE_API_KEY}"

            response = requests.get(search_url)
            if response.status_code == 200:
                search_data = response.json()
                if "items" in search_data:
                    video_ids = [item["id"]["videoId"] for item in search_data["items"] if "videoId" in item["id"]]

                    # Get detailed stats for these videos
                    if video_ids:
                        stats_url = f"https://www.googleapis.com/youtube/v3/videos?part=statistics&id={','.join(video_ids[:50])}&key={YOUTUBE_API_KEY}"
                        stats_response = requests.get(stats_url)

                        if stats_response.status_code == 200:
                            stats_data = stats_response.json()
                            stats_dict = {item["id"]: item.get("statistics", {}) for item in stats_data.get("items", [])}

                            for item in search_data["items"]:
                                if "videoId" in item["id"]:
                                    video_id = item["id"]["videoId"]
                                    stats = stats_dict.get(video_id, {})

                                    video_data = {
                                        "video_id": video_id,
                                        "title": item["snippet"]["title"],
                                        "channel": item["snippet"]["channelTitle"],
                                        "published_at": item["snippet"]["publishedAt"],
                                        "views": int(stats.get("viewCount", 0)),
                                        "likes": int(stats.get("likeCount", 0)),
                                        "comments": int(stats.get("commentCount", 0)),
                                        "category": f"movie_{query.replace(' ', '_')}",
                                        "extracted_at": datetime.now().isoformat()
                                    }
                                    videos.append(video_data)

            # Add small delay to avoid API rate limits
            import time
            time.sleep(1)

        logging.info(f"Total videos collected: {len(videos)}")

        if not videos:
            raise Exception("No videos were fetched from YouTube API")

        df = pd.DataFrame(videos)

        # Ensure data/raw directory exists
        raw_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "raw")
        os.makedirs(raw_dir, exist_ok=True)

        output_path = os.path.join(raw_dir, "youtube_raw.csv")

        # Check if file exists and append or create new
        if os.path.exists(output_path):
            # Read existing data to avoid duplicates
            try:
                existing_df = pd.read_csv(output_path)
                existing_video_ids = set(existing_df['video_id'].tolist()) if 'video_id' in existing_df.columns else set()

                # Filter out videos that already exist
                new_videos = [v for v in videos if v['video_id'] not in existing_video_ids]

                if new_videos:
                    new_df = pd.DataFrame(new_videos)
                    combined_df = pd.concat([existing_df, new_df], ignore_index=True)
                    combined_df.to_csv(output_path, index=False)
                    logging.info(f"Appended {len(new_videos)} new videos to existing CSV. Total: {len(combined_df)} videos")
                else:
                    logging.info("No new videos to add - all fetched videos already exist in CSV")
                    return True
            except Exception as e:
                logging.warning(f"Error reading existing CSV: {e}. Creating new file.")
                df = pd.DataFrame(videos)
                df.to_csv(output_path, index=False)
                logging.info(f"Created new CSV with {len(videos)} videos")
        else:
            # Create new file
            df = pd.DataFrame(videos)
            df.to_csv(output_path, index=False)
            logging.info(f"Created new CSV with {len(videos)} videos")

        logging.info(f"Successfully extracted {len(videos)} videos to {output_path}")
        return True

    except Exception as e:
        logging.error(f"Error in extract_youtube_data: {str(e)}")
        return False

if __name__ == "__main__":
    success = extract_youtube_data()
    sys.exit(0 if success else 1)