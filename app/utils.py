import os
from datetime import datetime
from .databases import schedule_db
import boto3
import ffmpeg
from .config import s3_client, BUCKET_NAME, BLANK_VIDEO_PATH, EVENT_FILE_DIR, OUTPUT_VIDEO_DIR,OUTPUT_VIDEO_DIR_FFMPEG
import subprocess


def generate_presigned_url(file_name):
    return s3_client.generate_presigned_url(
        'put_object',
        Params={'Bucket': BUCKET_NAME, 'Key': file_name},
        ExpiresIn=3600
    )

def generate_event_file(date):
    try:
        # Fetch schedule for the given date
        schedule = schedule_db.getByQuery({"date": date})
        events = schedule[0]['events'] if schedule else []

        lines = []
        # Start time at the beginning of the day
        current_time = datetime.combine(datetime.fromisoformat(date), datetime.min.time())

        for event in sorted(events, key=lambda e: e['start_time']):
            start_time = datetime.fromisoformat(event['start_time'])
            end_time = datetime.fromisoformat(event['end_time'])

            # Fill gap with blank videos if current time is before the event's start time
            if current_time < start_time:
                gap_duration = (start_time - current_time).total_seconds()
                full_blanks = int(gap_duration // 1)  # Number of 1-second blanks

                # Append full blank video repetitions
                for _ in range(full_blanks):
                    lines.append(f"file '{BLANK_VIDEO_PATH}'")
                    lines.append("duration 1")

            # Append the event video
            file_path = f"https://{BUCKET_NAME}.s3.amazonaws.com/{event['file_name']}"
            lines.append(f"file '{file_path}'")
            duration = (end_time - start_time).total_seconds()
            lines.append(f"duration {duration}")

            # Update current time to the end of the event
            current_time = end_time

        # Fill remaining time to the end of the day (23:59:59) with blank videos
        end_of_day = datetime.combine(datetime.fromisoformat(date), datetime.max.time())
        if current_time < end_of_day:
            remaining_duration = (end_of_day - current_time).total_seconds()
            full_blanks = int(remaining_duration // 1)

            # Append full blank video repetitions
            for _ in range(full_blanks):
                lines.append(f"file '{BLANK_VIDEO_PATH}'")
                lines.append("duration 1")

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
    
        if not os.path.exists(event_file_path):
            print(f"Event file not found for date {date}")
            return
        print(OUTPUT_VIDEO_DIR_FFMPEG)
          # FFmpeg command to start the stream
       # FFmpeg command to start the stream
        command = [
            'ffmpeg',
            '-f', 'concat',           # Concatenate video files
            '-safe', '0',             # Allow unsafe file paths
            '-i', event_file_path,    # Input event file
            '-c:v', 'libx264',        # Video codec
            '-preset', 'fast',        # Preset for encoding speed/quality trade-off
            '-f', 'hls',              # Output format (HTTP Live Streaming)
            f'{OUTPUT_VIDEO_DIR_FFMPEG}/{date}_stream.m3u8'  # Output file
        ]
        
        # Run the FFmpeg command
        result = subprocess.run(command, capture_output=True, text=True)
        
        # Check if the command was successful
        if result.returncode == 0:
            print("FFmpeg stream started successfully")
        else:
            print(f"FFmpeg failed with error: {result.stderr}")
    except Exception as e:
        print(f"Error starting FFmpeg stream: {e}")



def get_video_duration_from_s3(bucket_name, object_key):

    try:
        # Generate a pre-signed URL for the S3 object
        url = s3_client.generate_presigned_url('get_object',
                                               Params={'Bucket': bucket_name, 'Key': object_key},
                                               ExpiresIn=3600)  # URL valid for 1 hour

        # Use ffmpeg to probe the video stream directly from the S3 URL
        probe = ffmpeg.probe(url, v='error', select_streams='v:0', show_entries='stream=duration')

        # Extract the duration in seconds
        duration = float(probe['streams'][0]['duration'])
        
        print(f"The duration of the video is {duration:.2f} seconds")
        return duration
    except Exception as e:
        print(f"Error retrieving video duration: {e}")
