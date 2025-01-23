import boto3
import os
import time
import subprocess
from pathlib import Path
import threading
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from pysondb import db
from datetime import datetime
import pytz

LOCAL_TIMEZONE = pytz.timezone('Asia/Kolkata') 
schedule_db = db.getDb("schedule_db.json")
BLANK_VIDEO_PATH= f"https://tvunativeoverlay.s3.ap-south-1.amazonaws.com/concontent-scheduler/uploaded_videos/rsystems1crop.mp4"

# S3 setup
s3_client = boto3.client('s3')
BUCKET_NAME = 'tvunativeoverlay'
HLS_FOLDER = 'hls/'

# Path to the temporary directory for .ts files
TEMP_DIR = 'output_videos'
def generate_event_file(date,event_file_dir='event_files'):
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
                full_blanks = int(gap_duration // 20)  # Number of 1-second blanks

                # Append full blank video repetitions
                for _ in range(full_blanks):
                    lines.append(f"file '{BLANK_VIDEO_PATH}'")
                  #  lines.append("duration 1")
                    line_number += 2  # Each blank video adds 2 lines (file + duration)

            # Append the event video
            file_path = f"https://{BUCKET_NAME}.s3.amazonaws.com/{event['file_name']}"
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
            full_blanks = int(remaining_duration // 20)

            # Append full blank video repetitions
            for _ in range(full_blanks):
                lines.append(f"file '{BLANK_VIDEO_PATH}'")
  #              lines.append("duration 1")
                line_number += 2  # Each blank video adds 2 lines (file + duration)

        # Write the event file
        event_file_path = os.path.join(event_file_dir, f"{date}.txt")
        with open(event_file_path, 'w') as f:
            f.write('\n'.join(lines))

        print(f"Event file generated successfully: {event_file_dir}")
    except Exception as e:
        print(f"Error generating event file: {e}")

def generate_asset_file(file_path, lines_to_generate=1000):
    with open(file_path, 'w') as f:
        for i in range(1, lines_to_generate + 1):
           # if i % 1== 0:
                # Update with S3 video on every 10th line
                f.write(f"file 'https://tvunativeoverlay.s3.ap-south-1.amazonaws.com/test_video/rsystems2crop.mp4'\n")
           # else:
                # Use local video for the other lines
                f.write(f"file 'https://tvunativeoverlay.s3.ap-south-1.amazonaws.com/test_video/rsystems1crop.mp4'\n")
    
    print(f"Generated {file_path} with {lines_to_generate} lines.")

# Clear the S3 HLS folder before uploading new files
def clear_s3_folder():
    """Clear all objects in the S3 HLS folder before uploading new files."""
    try:
        # List all objects in the HLS folder
        response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=HLS_FOLDER)
        if 'Contents' in response:
            # Extract the keys of the objects to delete
            objects_to_delete = [{'Key': obj['Key']} for obj in response['Contents']]
            # Delete all objects in the folder
            s3_client.delete_objects(Bucket=BUCKET_NAME, Delete={'Objects': objects_to_delete})
            print(f"Cleared S3 folder {HLS_FOLDER}.")
        else:
            print(f"S3 folder {HLS_FOLDER} is already empty.")
    except Exception as e:
        print(f"Error clearing S3 folder {HLS_FOLDER}: {e}")


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


# FFmpeg Command
def run_ffmpeg(event_file_path, date):
    """Run the ffmpeg command to generate HLS segments and playlist."""
    # FFmpeg command to generate HLS
    ffmpeg_command = [
        'ffmpeg', '-protocol_whitelist', 'file,crypto,data,https,tls,tcp', '-re', '-f', 'concat', '-safe', '0', '-i', event_file_path,
        '-filter_complex', '[0:v]split=1[v1]; [v1]scale=w=854:h=480[v1out]',
        '-map', '[v1out]', '-c:v:0', 'libx264', '-b:v:0', '5000k', '-maxrate:v:0', '5350k',
        '-bufsize:v:0', '7500k', '-map', 'a:0', '-c:a', 'aac', '-b:a:0', '192k', '-ac', '2',
        '-f', 'hls', '-hls_time', '6', '-hls_playlist_type', 'event', '-hls_flags', 'independent_segments',
        '-hls_segment_type', 'mpegts', '-hls_segment_filename', os.path.join(TEMP_DIR, f'{date}_segment_%03d.ts'),
        '-master_pl_name', 'master.m3u8', '-var_stream_map', 'v:0,a:0', os.path.join(TEMP_DIR, f'{date}_playlist.m3u8')
    ]
 
    try:
        print(f"Running FFmpeg for {date}...")
        subprocess.run(ffmpeg_command, check=True)
        print(f"FFmpeg completed successfully for {date}")
    except subprocess.CalledProcessError as e:
        print(f"Error executing FFmpeg command: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")

# Helper function to check if a file is still being written
def is_file_written(file_path):
    """Check if the file is still being written (modified in the last few seconds)."""
    try:
        current_mod_time = os.path.getmtime(file_path)
        time.sleep(2)  # Sleep for 2 seconds to check if it's still being written
        new_mod_time = os.path.getmtime(file_path)
        return current_mod_time != new_mod_time  # True if file was modified in this time window
    except FileNotFoundError:
        return False

# Real-time file upload handler
class FileUploadHandler(FileSystemEventHandler):
    
    def on_moved(self, event):
        print(f'event type: {event.event_type}  path : {event.src_path}')
        if event.src_path.endswith('.tmp'):
            self.upload_file(event.src_path.split('.tmp')[0], '.m3u8')

    def on_closed(self, event):
        print(f'event type: {event.event_type}  path : {event.src_path}')
        """Upload new .ts files and .m3u8 playlist to S3 as they are created."""
        print(f"New file created: {event.src_path}")
        if event.src_path.endswith('.ts'):
            # Upload .ts segments
            self.upload_file(event.src_path, '.ts')
        
        elif event.src_path.endswith('.m3u8'):
            # Ensure playlist is uploaded, even if it's not fully written
            if event.src_path.endswith('.tmp'):
                print(f"Skipping .tmp file: {event.src_path}")
                return
            self.upload_file(event.src_path, '.m3u8')
            
    def upload_file(self, src_path, file_type):
        """Uploads a file to S3."""
        try:
            # Generate S3 key for the file
            s3_key = HLS_FOLDER + Path(src_path).name
            s3_client.upload_file(src_path, BUCKET_NAME, s3_key)
            print(f"Uploaded {file_type} file: {src_path} to s3://{BUCKET_NAME}/{s3_key}")
        except Exception as e:
            print(f"Error uploading {file_type} to S3: {e}")

  
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

# Function to run the ffmpeg command in the main thread
def start_ffmpeg(eventfile, date):
    run_ffmpeg(eventfile, date)


# Defining main function
def main():
   #Create directories for segments and playlists if they don't exist
    os.makedirs(TEMP_DIR, exist_ok=True)
    eventfile = 'event_files/2025-01-22.txt'
    date = '2025-01-22'
    #generate_asset_file(eventfile,3000)
    #generate_event_file(date)
    
    # Start the monitoring thread
    monitoring_thread = threading.Thread(target=start_file_monitoring)
    monitoring_thread.daemon = False  # Ensure the thread exits when the main program exits
    monitoring_thread.start()
    # Clear S3 folder (delete old files)
    clear_s3_folder()
    clear_output_folder()
    # Event file to process
    # Run ffmpeg to generate the video segments and playlist (main thread)
    start_ffmpeg(eventfile, date)
    # The script will continue running, allowing the monitoring thread to watch the directory
    monitoring_thread.join()


# Using the special variable 
# __name__
if __name__=="__main__":
    clear_s3_folder()
    #main()

