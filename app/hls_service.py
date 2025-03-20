import requests
from .config import API_BASE_URL,BUCKET_NAME


# Function to fetch all active fetchers
def fetch_active_fetchers():
    try:
        response = requests.get(API_BASE_URL)
        response.raise_for_status()
        fetchers = response.json()
        print("Active fetchers:", fetchers)
        return fetchers
    except requests.RequestException as e:
        print("Error fetching active fetchers:", e)
        return []

# Function to delete all active fetchers
def delete_all_fetchers():
    fetchers = fetch_active_fetchers()
    print("Deleting all fetchers...")
    if not fetchers:
        print("No fetchers to delete.")
        return

    for fetcher in fetchers:
        fetcher_id = fetcher["fetcherId"]
        try:
            response = requests.delete(f"{API_BASE_URL}/{fetcher_id}")
            response.raise_for_status()
            print(f"Fetcher with ID {fetcher_id} deleted successfully.")
        except requests.RequestException as e:
            print(f"Error deleting fetcher with ID {fetcher_id}:", e)

# Function to start a fetcher
def start_fetcher():
    delete_all_fetchers()
    fetcher_config = {
        "name": "eyevinn-s3",  # Unique name for the fetcher session
        "url": "http://video_scheduler_backend:5000/output_videos/master.m3u8",  # Source HLS stream
        "output": "s3",  # Plugin for S3
        "payload": {
            "bucket": BUCKET_NAME,  # Target S3 bucket name
            "folder": "hls-test",  # Folder within the S3 bucket
        },
        "windowSize": 240,  # Window size for the fetcher
       "concurrency": 40,  # Number of concurrent fetches
    }
    
    # fetcher_config = {
    #     "name": "Stream1",
    #     "url": "http://localhost:5000/output_videos/master.m3u8",
    #     "output": "mediapackage",
    #     "payload": {
    #         "ingestUrls": [ {
    #         "url": "https://b4fccfcab3a283c8.mediapackage.ap-south-1.amazonaws.com/in/v2/194f7ed2425a4c22816733d8f702b66c/194f7ed2425a4c22816733d8f702b66c/channel",
    #         "username": "ad2ac5ffa5c64906a52a5d1ae2c82483",
    #         "password": "46f9762c81f6473ebf44fe57d7bbb7a7"
    #         } ]
    #     }
    #     }

    try:
        print("Starting fetcher...")
        response = requests.post(
            API_BASE_URL,
            headers={"Content-Type": "application/json"},
            json=fetcher_config,
        )
        response.raise_for_status()
        print("Fetcher started successfully:", response.json())
    except requests.RequestException as e:
        print("Error starting fetcher:", e)

