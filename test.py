import boto3
import os
import time
import subprocess
from pathlib import Path
import threading
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# S3 setup
s3_client = boto3.client('s3')
BUCKET_NAME = 'logo-detection-bucket'
#BUCKET_NAME = 'tvunativeoverlay'
HLS_FOLDER = 'hls/'

# Path to the temporary directory for .ts files
TEMP_DIR = 'output_videos'

# def generate_asset_file(file_path, lines_to_generate=3000):
#     with open(file_path, 'w') as f:
#         for i in range(1, lines_to_generate + 1):
#             if i % 30 == 0:
#                 # Update with S3 video on every 10th line
#                 f.write(f"file 'https://logo-detection-bucket.s3.amazonaws.com/example_video.mp4'\n")
#             else:
#                  # Use local video for the other lines
#                  f.write(f"file 'blank_video/text_video.mp4'\n")
def generate_asset_file(file_path, lines_to_generate=3000):
    with open(file_path, 'w') as f:
        for i in range(1, lines_to_generate + 1):
            if i % 10 == 0:
                # Update with S3 video on every 10th line
                #f.write(f"file 'https://tvunativeoverlay.s3.ap-south-1.amazonaws.com/asset.mp4'\n")
                f.write(f"file 'https://logo-detection-bucket.s3.amazonaws.com/example_video.mp4'\n")
            else:
                # Use local video for the other lines
                f.write(f"file 'blank_video/text_video.mp4'\n")
    
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
    # Create directories for segments and playlists if they don't exist
    os.makedirs(TEMP_DIR, exist_ok=True)

    # FFmpeg command to generate HLS
    ffmpeg_command = [
        'ffmpeg', 
        '-protocol_whitelist', 'file,crypto,data,https,tls,tcp',
         '-re', 
        '-f', 'concat', '-safe', '0', 
        '-i', event_file_path,
           "-c:v", "libx264", 
           '-bufsize:v:0', '3500k',
            "-b:v", "1500k", 
            "-c:a", "aac", 
            "-b:a", "128k", 
            "-f", "hls", 
            "-hls_time", "6", 
            "-hls_playlist_type", "event", 
        '-hls_segment_filename', os.path.join(TEMP_DIR, f'{date}_segment_%03d.ts'),
        '-master_pl_name', 'master.m3u8', 
          os.path.join(TEMP_DIR, f'{date}_playlist.m3u8')
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

# # Real-time file upload handler
# class FileUploadHandler(FileSystemEventHandler):
#     def on_created(self, event):
#         """Upload new .ts files and .m3u8 playlist to S3 as they are created."""
#         print(f"New file created: {event.src_path}")
#         if event.src_path.endswith('.ts'):
#             # Upload .ts segments
#             self.upload_file(event.src_path, '.ts')
        
#         elif event.src_path.endswith('.m3u8'):
#             # Ensure playlist is uploaded, even if it's not fully written
#             if event.src_path.endswith('.tmp'):
#                 print(f"Skipping .tmp file: {event.src_path}")
#                 return
#             self.upload_file(event.src_path, '.m3u8')
#         elif event.src_path.endswith('.tmp'):
#              self.upload_file(event.src_path.split('.tmp')[0], '.tmp')
            
#     def upload_file(self, src_path, file_type):
#         """Uploads a file to S3."""
#         try:
#             # Generate S3 key for the file
#             s3_key = HLS_FOLDER + Path(src_path).name
#             s3_client.upload_file(src_path, BUCKET_NAME, s3_key)
#             print(f"Uploaded {file_type} file: {src_path} to s3://{BUCKET_NAME}/{s3_key}")
#         except Exception as e:
#             print(f"Error uploading {file_type} to S3: {e}")

#     def on_modified(self, event):
#         """Re-upload the master.m3u8 file if modified."""
#         if event.src_path.endswith('.m3u8'):
#             if not is_file_written(event.src_path):
#                 s3_playlist_key = HLS_FOLDER + Path(event.src_path).name
#                 try:
#                     s3_client.upload_file(event.src_path, BUCKET_NAME, s3_playlist_key)
#                     print(f"Re-uploaded master.m3u8: {event.src_path} to s3://{BUCKET_NAME}/{s3_playlist_key}")
#                 except Exception as e:
#                     print(f"Error re-uploading master.m3u8 to S3: {e}")

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

# Function to start the file monitoring in a separate thread
def start_monitoring_thread():
    monitoring_thread = threading.Thread(target=start_file_monitoring)
    monitoring_thread.daemon = True  # Ensure the thread exits when the main program exits
    monitoring_thread.start()

# Function to run the ffmpeg command in the main thread
def start_ffmpeg(eventfile, date):
    run_ffmpeg(eventfile, date)



eventfile = 'event_files/2025-01-16.txt'
date = '2025-01-16'
generate_asset_file(eventfile,3000)

# Start the monitoring thread
start_monitoring_thread()

# Clear S3 folder (delete old files)
clear_s3_folder()

# Clear local output folder
clear_output_folder()

# Event file to process


# Run ffmpeg to generate the video segments and playlist (main thread)
start_ffmpeg(eventfile, date)

# The script will continue running, allowing the monitoring thread to watch the directory
