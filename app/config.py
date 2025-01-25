import boto3
import os

AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")

s3_client =  boto3.client("s3", region_name=AWS_REGION, endpoint_url='https://s3.ap-south-1.amazonaws.com')  
BUCKET_NAME = 'pocrsibucket'
BLANK_VIDEO_PATH = 'blank_video/text_video.mp4'
EVENT_FILE_DIR = './event_files/'
OUTPUT_VIDEO_DIR = './output_videos/'
OUTPUT_VIDEO_DIR_FFMPEG = 'output_videos'
upload_video_folder = 'content-scheduler/uploaded_videos'

# API Base URL for the fetcher service
API_BASE_URL = "http://localhost:8080/api/v1/fetcher"