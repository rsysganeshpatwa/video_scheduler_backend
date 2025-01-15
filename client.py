import requests
import json
from datetime import datetime
from app.config import BUCKET_NAME

# Define base URL (change this to your local or deployed API URL)
BASE_URL = "http://localhost:5000"  # Replace with your API URL if it's deployed
 
 #unction to test generate-presigned-url API
def get_presigned_url_for_upload(file_name):
    url = f"{BASE_URL}/generate-presigned-url"
    data = {
        "file_name": file_name
    }

    response = requests.post(url, json=data)
    if response.status_code == 200:
        print("Presigned URL:", response.json()['url'])
    else:
        print(f"Error: {response.json()}")
    return response.json()['url'] if response.status_code == 200 else None
# Function to test add-metadata API
def test_add_metadata(file_name, bucket_name, duration=None):
    url = f"{BASE_URL}/add-metadata"
    data = {
        "file_name": file_name,
        "bucket_name": bucket_name,
        "duration": duration  # Optional
    }

    response = requests.post(url, json=data)
    if response.status_code == 200:
        print("Metadata added successfully")
    elif response.status_code == 409:
        print("Metadata already exists")
    else:
        print(f"Error: {response.json()}")

# Function to test schedule-video API
def test_schedule_video(file_name, start_time):
    url = f"{BASE_URL}/schedule-video"
    data = {
        "file_name": file_name,
        "start_time": start_time.isoformat()  # Convert datetime to ISO format
    }

    response = requests.post(url, json=data)
    if response.status_code == 200:
        print("Video scheduled successfully")
    else:
        print(f"Error: {response.json()}")

# Function to upload video file to S3 using presigned URL
def upload_video_to_s3(presigned_url, video_path):
    try:
        with open(video_path, 'rb') as video_file:
            headers = {
                'Content-Type': 'video/mp4'  # Ensure you set the correct MIME type for your video
            }
            # Upload the video file to the presigned URL using PUT request
            response = requests.put(presigned_url, data=video_file, headers=headers, verify=False)
            if response.status_code == 200:
                print("Video uploaded successfully!")
            else:
                print(f"Error uploading video: {response.status_code}, {response.text}")
    except Exception as e:
        print(f"Error reading or uploading video file: {e}")

# Example usage:
if __name__ == "__main__":
       # Step 1: Get presigned URL
    file_name = "example_video.mp4"  # Replace with your video file name
    #presigned_url = get_presigned_url_for_upload(file_name)
    
    # Step 2: Upload video if presigned URL is obtained
    #if presigned_url:
    #    video_path = "blank_video/example_video.mp4"  # Local video file path
    #    upload_video_to_s3(presigned_url, video_path)

    # Test add-metadata (with or without duration)
    #test_add_metadata(file_name, BUCKET_NAME, duration=3600)  # You can omit "duration" if you want to fetch it from S3

    # Test schedule-video
    test_schedule_video(file_name, datetime(2025, 1, 15, 22, 37))  # Example start time
