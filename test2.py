import boto3
import os
import time
import subprocess
from datetime import datetime
from pysondb import db
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import threading
import signal

# Load schedule database
schedule_db = db.getDb("schedule_db.json")

# S3 setup
s3_client = boto3.client('s3')
BUCKET_NAME = 'tvunativeoverlay'
HLS_FOLDER = 'hls/'

# Temporary directory for .ts files
TEMP_DIR = 'output_videos'

def delete_s3_files_last_30_seconds():
    """Delete S3 .ts files created in the last 30 seconds."""
    try:
        current_time = datetime.now()
        response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=HLS_FOLDER)
        if 'Contents' in response:
            for obj in response['Contents']:
                if obj['Key'].endswith('.ts'):  # Check for .ts files only
                    last_modified = obj['LastModified']
                    # Check if the file was modified in the last 30 seconds
                    if (current_time - last_modified).total_seconds() <= 30:
                        s3_client.delete_object(Bucket=BUCKET_NAME, Key=obj['Key'])
                        print(f"Deleted S3 file: {obj['Key']}")
    except Exception as e:
        print(f"Error deleting S3 .ts files: {e}")


# Clear S3 folder
def clear_s3_folder():
    """Clear all objects in the S3 HLS folder."""
    try:
        response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=HLS_FOLDER)
        if 'Contents' in response:
            objects_to_delete = [{'Key': obj['Key']} for obj in response['Contents']]
            s3_client.delete_objects(Bucket=BUCKET_NAME, Delete={'Objects': objects_to_delete})
            print(f"Cleared S3 folder {HLS_FOLDER}.")
        else:
            print(f"S3 folder {HLS_FOLDER} is already empty.")
    except Exception as e:
        print(f"Error clearing S3 folder {HLS_FOLDER}: {e}")

# Clear local output folder
def clear_output_folder():
    """Clear the temporary output folder."""
    try:
        for file in os.listdir(TEMP_DIR):
            file_path = os.path.join(TEMP_DIR, file)
            if os.path.isfile(file_path):
                os.unlink(file_path)
        print(f"Cleared local folder {TEMP_DIR}.")
    except Exception as e:
        print(f"Error clearing local folder {TEMP_DIR}: {e}")

# Run FFmpeg command
def run_ffmpeg(video_path, date):
    """Run the FFmpeg command for a single video."""
    os.makedirs(TEMP_DIR, exist_ok=True)
    print(f"Processing video: {video_path}")
    ffmpeg_command = [
            'ffmpeg', 
            '-re', 
            '-i', video_path,  # Input video
            '-filter_complex', '[0:v]split=1[v1];[v1]scale=w=640:h=360[v1out]',  # Split and scale video
            '-map', '[v1out]',  # Map the scaled video
            '-c:v', 'libx264',  # Use libx264 codec
            '-b:v', '5000k',  # Video bitrate
            '-maxrate', '5350k',  # Max video bitrate
            '-bufsize', '7500k',  # Buffer size
            '-map', 'a:0',  # Map audio stream
            '-c:a', 'aac',  # Audio codec
            '-b:a', '192k',  # Audio bitrate
            '-ac', '2',  # Number of audio channels
            '-f', 'hls',  # Format output as HLS
            '-hls_time', '6',  # Segment length (in seconds)
            '-hls_playlist_type', 'event',  # Event-based playlist
            '-hls_flags', 'independent_segments',  # Independent segments
            '-hls_segment_type', 'mpegts',  # Segment type
            '-hls_segment_filename', os.path.join(TEMP_DIR, f'{date}_segment_%03d.ts'),  # Output segment pattern
            '-master_pl_name', 'master.m3u8',  # Master playlist filename
            '-var_stream_map', 'v:0,a:0',  # Map the video and audio stream
            os.path.join(TEMP_DIR, f'{date}_playlist.m3u8')  # Output playlist filename
        ]


    try:
        print(f"Running FFmpeg for video: {video_path}")
        subprocess.run(ffmpeg_command, check=True)
        
        print("FFmpeg processing completed.")
    except subprocess.CalledProcessError as e:
        print(f"Error during FFmpeg execution: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")

# Monitor the schedule and start FFmpeg processing
def monitor_schedule():
    """Continuously monitor the schedule database and process events as they become active."""
    processed_events = set()  # Track processed events to avoid duplicate processing

    try:
        while True:
            # Reload schedule data
            schedule_data = schedule_db.getAll()
            current_time = datetime.now()

            for schedule in schedule_data:
                date = schedule['date']
                for event in schedule['events']:
                    start_time = datetime.strptime(event['start_time'], '%Y-%m-%d %H:%M:%S')

                    # Check if the event is currently active and not yet processed
                    print(f"Current time: {current_time}, Event time: {start_time}")
                    print(f'start_time.date(){start_time.date() }={current_time.date()} and start_time <= current_time={start_time <= current_time} and start_time not in processed_events={start_time not in processed_events}')
                if start_time.date() == current_time.date() and start_time <= current_time and start_time not in processed_events:
                        video_path = f"https://{BUCKET_NAME}.s3.ap-south-1.amazonaws.com/{event['file_name']}"
                        # Run FFmpeg for the current event
                        run_ffmpeg(video_path, date)

                        # Mark the event as processed
                        processed_events.add(start_time)

            time.sleep(3)  # Check every 3 seconds for new events

    except Exception as e:
        print(f"Error in schedule monitoring: {e}")

class FileUploadHandler(FileSystemEventHandler):
    def on_moved(self, event):
        print(f'event type: {event.event_type}  path : {event.src_path}')
        if event.src_path.endswith('.tmp'):
            self.upload_file(event.src_path.split('.tmp')[0], '.m3u8')

    def on_created(self, event):
        print(f'event type: {event.event_type}  path : {event.src_path}')
        """Upload new .ts files and .m3u8 playlist to S3 as they are created."""
        if event.src_path.endswith('.ts'):
            # Upload .ts segments
            self.upload_file(event.src_path, '.ts')
        
        elif event.src_path.endswith('.m3u8'):
            # Ensure playlist is uploaded, even if it's not fully written
            if event.src_path.endswith('.tmp'):
                print(f"Skipping .tmp file: {event.src_path}")
                return
            self.upload_file(event.src_path, '.m3u8')

        # Trigger cleanup after each file is processed
        #self.cleanup_old_files()

    def upload_file(self, src_path, file_type):
        """Uploads a file to S3."""
        try:
            # Generate S3 key for the file
            s3_key = os.path.join(HLS_FOLDER, Path(src_path).name)
            s3_client.upload_file(src_path, BUCKET_NAME, s3_key)
            print(f"Uploaded {file_type} file: {src_path} to s3://{BUCKET_NAME}/{s3_key}")
        except Exception as e:
            print(f"Error uploading {file_type} to S3: {e}")

    # def cleanup_old_files(self):
    #     """Delete local and S3 files generated in the last 30 seconds."""
    #     delete_s3_files_last_30_seconds()

# Start monitoring the temporary directory for new files
def start_file_monitoring():
    """Start monitoring the temporary directory for new .ts and .m3u8 files."""
    event_handler = FileUploadHandler()
    observer = Observer()
    observer.schedule(event_handler, TEMP_DIR, recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

# Graceful shutdown of the script
def handle_shutdown(signum, frame):
    print("Shutting down gracefully...")
    exit(0)

# Main execution
if __name__ == '__main__':
    # Setup signal handler for graceful shutdown
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    # Clear previous data
    clear_output_folder()
    clear_s3_folder()

    # Start monitoring the schedule
    schedule_thread = threading.Thread(target=monitor_schedule)
    schedule_thread.start()

    # # Start monitoring for file creation in TEMP_DIR
    file_monitor_thread = threading.Thread(target=start_file_monitoring)
    file_monitor_thread.start()

    # Join threads to ensure they run concurrently
    schedule_thread.join()
    file_monitor_thread.join()
