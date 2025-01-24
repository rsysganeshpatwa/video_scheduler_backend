import os
import time
import subprocess
import threading
from pathlib import Path
from datetime import datetime, timedelta
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import boto3


# S3 setup
s3_client = boto3.client('s3')
BUCKET_NAME = 'tvunativeoverlay'
HLS_FOLDER = 'hls/'

# Path to the temporary directory for .ts files
TEMP_DIR = 'output_videos'
EVENT_FILE_DIR = 'event_files'

# Globals for process and thread management
ffmpeg_process = None
monitoring_thread = None
stop_event = threading.Event()


# File Monitoring Class
class FileUploadHandler(FileSystemEventHandler):
    
    
    def on_created(self, event):
        print(f'event type: {event.event_type}  path : {event.src_path}')
        if event.src_path.endswith('.ts'):
            # Upload .ts segments
            self.upload_file(event.src_path, '.ts')
        
        elif event.src_path.endswith('.m3u8'):
            # Ensure playlist is uploaded, even if it's not fully written
            if event.src_path.endswith('.tmp'):
                print(f"Skipping .tmp file: {event.src_path}")
                return
            self.upload_file(event.src_path, '.m3u8')
        
   
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
# Utility Functions
def clear_s3_folder():
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


def clear_output_folder():
    try:
        for file in os.listdir(TEMP_DIR):
            file_path = os.path.join(TEMP_DIR, file)
            if os.path.isfile(file_path):
                os.unlink(file_path)
        print(f"Cleared local folder {TEMP_DIR}.")
    except Exception as e:
        print(f"Error clearing local folder {TEMP_DIR}: {e}")


def delete_last_30_seconds_files():
    """Delete files created in the last 30 seconds from the local folder and S3."""
    now = datetime.now()
    cutoff_time = now - timedelta(seconds=30)

    # Delete local files
    try:
        for file in os.listdir(TEMP_DIR):
            file_path = os.path.join(TEMP_DIR, file)
            file_creation_time = datetime.fromtimestamp(os.path.getctime(file_path))
            if file_creation_time > cutoff_time:
                os.unlink(file_path)
                print(f"Deleted local file: {file_path}")
    except Exception as e:
        print(f"Error deleting local files: {e}")

    # Delete files from S3
    try:
        response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=HLS_FOLDER)
        if 'Contents' in response:
            for obj in response['Contents']:
                s3_key = obj['Key']
                file_creation_time = obj['LastModified']
                if file_creation_time > cutoff_time:
                    s3_client.delete_object(Bucket=BUCKET_NAME, Key=s3_key)
                    print(f"Deleted S3 file: s3://{BUCKET_NAME}/{s3_key}")
    except Exception as e:
        print(f"Error deleting S3 files: {e}")


def start_file_monitoring():
    global stop_event

    def monitor():
        observer = Observer()
        event_handler = FileUploadHandler()
        observer.schedule(event_handler, TEMP_DIR, recursive=False)
        observer.start()

        try:
            while not stop_event.is_set():
                time.sleep(1)
        finally:
            observer.stop()
            observer.join()

    monitoring_thread = threading.Thread(target=monitor)
    monitoring_thread.start()
    return monitoring_thread


# FFmpeg Process Functions
def run_ffmpeg(event_file, date):
    global ffmpeg_process

    ffmpeg_command = [
        'ffmpeg', '-protocol_whitelist', 'file,crypto,data,https,tls,tcp', '-re', '-f', 'concat', '-safe', '0', '-i', event_file,
        '-filter_complex', '[0:v]split=1[v1]; [v1]scale=w=854:h=480[v1out]',
        '-map', '[v1out]', '-c:v:0', 'libx264', '-b:v:0', '5000k', '-maxrate:v:0', '5350k',
        '-bufsize:v:0', '3500k', '-map', 'a:0', '-c:a', 'aac', '-b:a:0', '192k', '-ac', '2',
        '-f', 'hls', '-hls_time', '6', 
        '-hls_list_size','20',
     '-hls_flags' ,'delete_segments',
  '-hls_delete_threshold', '20',
        '-hls_segment_type', 'mpegts', '-hls_segment_filename', os.path.join(TEMP_DIR, f'{date}_segment_%03d.ts'),
        '-master_pl_name', 'master.m3u8', '-var_stream_map', 'v:0,a:0', os.path.join(TEMP_DIR, f'{date}_playlist.m3u8')
    ]

    try:
        print(f"Starting FFmpeg for {date}...")
        ffmpeg_process = subprocess.Popen(ffmpeg_command)
        #ffmpeg_process.wait()
        print(f"FFmpeg process completed for {date}.")
    except Exception as e:
        print(f"Error in FFmpeg process: {e}")


def stop_ffmpeg():
    global ffmpeg_process
    if ffmpeg_process and ffmpeg_process.poll() is None:
        try:
            print("Stopping FFmpeg process...")
            ffmpeg_process.terminate()
            ffmpeg_process.wait(timeout=5)
            print("FFmpeg process stopped successfully.")
        except subprocess.TimeoutExpired:
            print("FFmpeg did not terminate in time. Forcibly killing it.")
            ffmpeg_process.kill()
        except Exception as e:
            print(f"Error stopping FFmpeg: {e}")
        finally:
            ffmpeg_process = None
    else:
        print("No FFmpeg process to stop.")


# Main Streaming Control Functions
def start_stream(date):
    global monitoring_thread, stop_event

    os.makedirs(TEMP_DIR, exist_ok=True)
    event_file = f'{EVENT_FILE_DIR}/{date}.txt'

    

    # Stop previous operations
    stop_ffmpeg()
    stop_event.set()
    
    clear_s3_folder()
    clear_output_folder()
    if monitoring_thread and monitoring_thread.is_alive():
        monitoring_thread.join()

    # Start monitoring and FFmpeg
    stop_event.clear()
    monitoring_thread = start_file_monitoring()
    threading.Thread(target=run_ffmpeg, args=(event_file, date)).start()


def stop_stream():
    global monitoring_thread, stop_event

    stop_ffmpeg()
    stop_event.set()
    if monitoring_thread and monitoring_thread.is_alive():
        monitoring_thread.join()
        monitoring_thread = None
