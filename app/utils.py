import os
import logging
from datetime import datetime
from .databases import schedule_db
import boto3
import ffmpeg
from .config import s3_client, BUCKET_NAME, EVENT_FILE_DIR, OUTPUT_VIDEO_DIR,OUTPUT_VIDEO_DIR_FFMPEG,upload_video_folder
import subprocess
from .databases import metadata_db
import pytz
import json
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

LOCAL_TIMEZONE = pytz.timezone('Asia/Kolkata')  # Replace with your desired timezone
# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
#BLANK_VIDEO_PATH= f"https://{BUCKET_NAME}.s3.ap-south-1.amazonaws.com/{upload_video_folder}/blank_video.mp4"
BLANK_VIDEO_PATH= f"{upload_video_folder}/blank_video.mp4"

def generate_presigned_url_func(file_name):
    return s3_client.generate_presigned_url(
        'put_object',
        Params={'Bucket': BUCKET_NAME, 'Key': file_name},
        ExpiresIn=3600
    )
# Assuming you want to work with your local server time or a specific timezone


def generate_event_file(date):
    try:
        # Fetch schedule for the given date
        schedule = schedule_db.getByQuery({"date": date})
        events = schedule[0]['events'] if schedule else []

        lines = []
        line_number = 1  # Initialize line number

        # Get the current time in the specified timezone and format
        now = datetime.now(LOCAL_TIMEZONE)
        current_time_24hr = now.strftime('%Y-%m-%d %H:%M:%S')
        current_time = datetime.strptime(current_time_24hr, '%Y-%m-%d %H:%M:%S')
        current_time = LOCAL_TIMEZONE.localize(current_time)
        print(f"Current Time: {current_time}")

        for event in sorted(events, key=lambda e: e['start_time']):
            start_time = datetime.fromisoformat(event['start_time'])
            end_time = datetime.fromisoformat(event['end_time'])

            # Localize event times to the same timezone
            start_time = LOCAL_TIMEZONE.localize(start_time)
            end_time = LOCAL_TIMEZONE.localize(end_time)

            # Skip event if it's already in the past
            if start_time < current_time:
                print(f"Skipping event {event['file_name']} as it is in the past ({start_time})")
                continue  # Skip the event if its start time is in the past

            # Fill gap with blank videos if current time is before the event's start time
            if current_time < start_time:
                gap_duration = (start_time - current_time).total_seconds()
                full_blanks = int(gap_duration // 10)  # Number of 1-second blanks

                # Append full blank video repetitions
                for _ in range(full_blanks):
                    lines.append(f"file '{BLANK_VIDEO_PATH}'")
                  #  lines.append("duration 1")
                    line_number += 2  # Each blank video adds 2 lines (file + duration)

            # Append the event video
            #file_path = f"https://{BUCKET_NAME}.s3.ap-south-1.amazonaws.com/{event['file_name']}"
            file_path = f"{event['file_name']}"
            lines.append(f"file '{file_path}'")
#            duration = (end_time - start_time).total_seconds()
 #           lines.append(f"duration {duration}")

            print(f"Added event video: {file_path} at line {line_number}, current time: {current_time}")
            line_number += 2  # Event video adds 2 lines (file + duration)

            # Update current time to the end of the event
            current_time = end_time

        # Fill remaining time to the end of the day (23:59:59) with blank videos
        end_of_day = datetime.combine(datetime.fromisoformat(date), datetime.max.time())
        end_of_day = LOCAL_TIMEZONE.localize(end_of_day)
        print(f"End of Day: {end_of_day}")

        if current_time < end_of_day:
            remaining_duration = (end_of_day - current_time).total_seconds()
            full_blanks = int(remaining_duration // 10)

            # Append full blank video repetitions
            for _ in range(full_blanks):
                lines.append(f"file '{BLANK_VIDEO_PATH}'")
  #              lines.append("duration 1")
                line_number += 2  # Each blank video adds 2 lines (file + duration)

        # Write the event file
        event_file_path = os.path.join(EVENT_FILE_DIR, f"{date}.txt")
        with open(event_file_path, 'w') as f:
            f.write('\n'.join(lines))

        print(f"Event file generated successfully: {event_file_path}")
    except Exception as e:
        print(f"Error generating event file: {e}")

# Function to start FFmpeg stream
def start_stream(date):
    try:
        event_file_path = os.path.join(EVENT_FILE_DIR, f"{date}.txt")

        logger.info(f"Starting FFmpeg stream for date: {date}")
    
     
    except Exception as e:
        print(f"Error starting FFmpeg stream: {e}")

def get_video_duration_from_s3(bucket_name, object_key):
    try:
        # Generate a pre-signed URL for the S3 object
        print(f"Bucket name: {bucket_name}")
        print(f"Object key: {object_key}")
        url = s3_client.generate_presigned_url('get_object',
                                               Params={'Bucket': bucket_name, 'Key': object_key},
                                               ExpiresIn=3600)  # URL valid for 1 hour

        # Use subprocess to call ffprobe on the URL
        print(f"Probing video at URL: {url}")
        result = subprocess.run(
            [
                'ffprobe',
                '-v', 'error',
                '-select_streams', 'v:0',
                '-show_entries', 'stream=duration',
                '-of', 'json',
                url
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )

        # Parse the ffprobe output
        if result.returncode != 0:
            raise Exception(f"ffprobe error: {result.stderr.decode('utf-8')}")

        probe_data = json.loads(result.stdout)
        duration = float(probe_data['streams'][0]['duration'])

        print(f"The duration of the video is {duration:.2f} seconds")
        return duration

    except Exception as e:
        print(f"Error retrieving video duration: {e}")
        return None

def add_metadata(file_name, bucket_name, duration):
    try:
        # Check if the file already exists in the database
        existing_metadata = metadata_db.getByQuery({"file_name": file_name})
        if existing_metadata:
            print(f"Metadata for {file_name} already exists.")
            return False

        # Add the metadata to the database
        metadata_db.add({
            "file_name": file_name,
            "bucket_name": bucket_name,
            "duration": duration
        })
        print(f"Metadata for {file_name} added successfully.")
        return True
    except Exception as e:
        print(f"Error adding metadata: {e}")
        return False

