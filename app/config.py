import boto3

s3_client = boto3.client('s3')
BUCKET_NAME = 'logo-detection-bucket'
BLANK_VIDEO_PATH = 'blank_video/text_video.mp4'
EVENT_FILE_DIR = './event_files/'
OUTPUT_VIDEO_DIR = './output_videos/'
OUTPUT_VIDEO_DIR_FFMPEG = 'output_videos'