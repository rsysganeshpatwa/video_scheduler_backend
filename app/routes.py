from flask import Blueprint, request, jsonify,send_from_directory
from datetime import datetime, timedelta
from video_scheduler_backend.app.scheduler import schedule_stream_job
from .databases import metadata_db, schedule_db
from .utils import generate_event_file, get_video_duration_from_s3, generate_presigned_url_func, add_metadata
from .config import s3_client, BUCKET_NAME,upload_video_folder
import ffmpeg
import os
import urllib.parse
from .ffmpeg_service import start_ffmpeg_service
import json
import pytz
import subprocess

routes = Blueprint('routes', __name__)
# Path to the JSON file
STREAM_DB_FILE = "stream_db.json"  # 🔹 Path to your JSON database
# Define the India time zone
india_tz = pytz.timezone('Asia/Kolkata')
# Path to the 'output_videos' directory
OUTPUT_VIDEOS_DIR = os.path.join(os.getcwd(), "output_videos")
FFMPEG_PATH = "C:\\Program Files\\ffmpeg\\bin\\ffmpeg.exe" # Update this path
active_streams = {}  # ✅ Store active stream statuses
stream_status = {"is_streaming": False}
# Ensure the directory existsif os.path.exists(JSON_FILE):

if not os.path.exists(OUTPUT_VIDEOS_DIR):
    os.makedirs(OUTPUT_VIDEOS_DIR)

# Define default route
# Path to your video files
VIDEO_FOLDER = os.path.join(os.getcwd(), "event_files", "content-scheduler", "uploaded_videos")
UPLOAD_FOLDER = 'uploads'
processes = {}

if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

@routes.route('/')
def home():
    return "Welcome to the Video Scheduler API! The server is running."

@routes.route('/videos/<filename>')
def get_video_file(filename):
    """Serve video files dynamically"""
    files = os.listdir(VIDEO_FOLDER)
    if filename not in files:
        return jsonify({"error": "File not found", "available_files": files}), 404
    return send_from_directory(VIDEO_FOLDER, filename)

@routes.route('/videos')
def list_videos():
    # return jsonify({"videos": video_files})
    try:
        """Get a list of all video files"""
        files = os.listdir(VIDEO_FOLDER)
        data = [file for file in files if file.endswith((".mp4", ".avi", ".mkv", ".mov"))]  # Filter video files
        print(data)
        # update file_name 
        
        # add one more field bucket file url
        # for data in metadata_json:
        #     data['file_url'] = f"https://{BUCKET_NAME}.s3.amazonaws.com/{data['file_name']}"
        #     file_name = os.path.basename(data['file_name'])
        #     data['file_name'] = os.path.splitext(file_name)[0]

        return jsonify(data), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500
def save_stream_data(data):
    """🔹 Save stream data to JSON file."""
    with open(STREAM_DB_FILE, "w") as file:
        json.dump(data, file, indent=4)  # 🔹 Save back to file

def actual_start_stream(data):
    """
    Starts the stream using FFmpeg.
    If data["sourceType"] is "list", use the local file as input.
    Otherwise, use an HLS URL as input.
    """
    # Set stream status to true
    stream_status["is_streaming"] = True
    print("Received Data:", data)

    # Initialize list to store active process information
    active_processes = []

    # Check the sourceType from the payload
    if data.get("sourceType") == "list":
        input_filename = data.get("selectedSource")
        input_url = os.path.join(VIDEO_FOLDER, input_filename)
        print(f"Full Video Path: {input_url}")
        if not os.path.exists(input_url):
            return jsonify({"error": f"File not found at {input_url}"}), 400

        # Process each stream from the streams list
        streams = data.get("streams", [])
        for stream in streams:
            stream_id = stream.get("id")
            output_url = stream.get("url")
            if not output_url:
                continue  # Skip if no output URL
            if stream_id in processes:
                return jsonify({"error": f"Stream {stream_id} already running."}), 400

            # Build FFmpeg command for local file input
            ffmpeg_cmd = [
                "ffmpeg",
                "-re",
                "-i", input_url,                     # Local file input
                "-c:v", "libx264",
                "-preset", "veryfast",
                "-b:v", "1500k",
                "-maxrate", "1500k",
                "-bufsize", "3000k",
                "-pix_fmt", "yuv420p",
                "-g", "50",                          # Keyframe interval (adjust based on fps)
                "-c:a", "aac",
                "-b:a", "128k",
                "-ar", "44100",
                "-f", "flv",
                "-rtmp_buffer", "3000",
                "-rtmp_live", "live",
                "-reconnect", "1",
                "-reconnect_at_eof", "1",
                "-reconnect_streamed", "1",
                output_url                          # Destination RTMP URL
            ]

            process = subprocess.Popen(ffmpeg_cmd)
            processes[stream_id] = process
            active_processes.append({"streamId": stream_id, "outputUrl": output_url})
        
        # Save record to JSON DB
        save_stream_data({
            "date": data.get("date"),
            "scheduleType": data.get("scheduleType"),
            "time": data.get("time"),
            "sourceType": data.get("sourceType"),
            "selectedSource": input_filename,
            "streams": active_processes
        })

        return jsonify({
            "message": "Streams started!",
            "is_streaming": True,
            "streams": active_processes
        })

    else:
        # For other source types (e.g. HLS input), use the alternative command.
        # In this example, the HLS URL is hardcoded.
        ffmpeg_cmd = [
            "ffmpeg",
            "-i", "rtmp://ec2-3-108-220-57.ap-south-1.compute.amazonaws.com/live/stream",
            "-c:v", "copy",
            "-c:a", "copy",
            "-f", "flv",
            "rtmp://13.60.60.72/live/sZFLnowV9aKb"
        ]

        ffmpeg_cmd = [
            "ffmpeg",
            "-i", "rtmp://ec2-3-108-220-57.ap-south-1.compute.amazonaws.com/live/stream",
            "-c:v", "copy",
            "-c:a", "copy",
            "-f", "tee",
            "[f=flv]rtmp://13.60.60.72/live/sZFLnowV9aKb|[f=flv]rtmp://live-api-s.facebook.com:80/rtmp/FB-3925227551052370-0-Ab2ba39_Kf8Ws9qUVbYLzp07|[f=flv]rtmp://live.linkedin.com/live/your_linkedin_stream_key"
        ]
        
        process = subprocess.Popen(ffmpeg_cmd)
        # Use a default key since there's no dynamic stream id
        processes["default"] = process
        save_stream_data({
            "date": data.get("date"),
            "scheduleType": data.get("scheduleType"),
            "time": data.get("time"),
            "sourceType": data.get("sourceType"),
            "selectedSource": data.get("selectedSource"),
            "streams": [{"streamId": "default", "outputUrl": "rtmp://13.60.60.72/live/Ki2YNXzsCA95"}]
        })
        return jsonify({
            "message": "Stream started with HLS input!",
            "is_streaming": True,
            "streams": [{"streamId": "default", "outputUrl": "rtmp://13.60.60.72/live/Ki2YNXzsCA95"}]
        })
@routes.route('/start-stream', methods=['POST'])
def start_stream():
    data = request.json  # ✅ Get request data
    stream_status["is_streaming"] = True
 
    print("Received Data:", data)  # ✅ Debugging
 
    streams = data.get("streams", [])  # ✅ Get all streams (array of {id, title, url})
    input_url = data.get("selectedSource")  # ✅ Get input file name
    source_type = data.get("sourceType")  # ✅ Get source type 
    if  source_type == "list":
        input_url = os.path.join(VIDEO_FOLDER, input_url)  # ✅ Construct path
        if not os.path.exists(input_url):
            return jsonify({"error": f"File not found at {input_url}"}), 400  # ✅ File check
 
    print(f"Full Video Path: {input_url}");  # ✅ Debugging step
    
    if not streams:
        return jsonify({"error": "No output streams provided"}), 400  # ✅ Check if streams exist
 
    stream_id = "-".join([stream["id"] for stream in streams])  # Combine stream IDs
    output_urls = [stream["url"] for stream in streams if "url" in stream]
 
    if stream_id in processes:
        return jsonify({"error": f"Streams with ID {stream_id} are already running."}), 400
 
    # ✅ Construct `tee` output string for multiple streams
    tee_output = "|".join([f"[f=flv]{url}" for url in output_urls])
 
    # ✅ FFmpeg command using `tee` for multiple RTMP outputs
    ffmpeg_cmd = [
        "ffmpeg", "-re", "-i", input_url,
        "-c:v", "libx264", "-preset", "veryfast", "-b:v", "750K", "-maxrate", "750K", "-bufsize", "1500K",
        "-c:a", "aac", "-b:a", "96K", "-ar", "44100",
        "-f", "tee", tee_output
    ]
 
    print(f"Starting stream: {ffmpeg_cmd}")  # ✅ Debugging
    process = subprocess.Popen(ffmpeg_cmd)
    # Capture output & error logs
    process = subprocess.Popen(
        ffmpeg_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )
 
    # Read output & error in real-time
    for line in process.stderr:
        print(line.strip())  # ✅ Print FFmpeg error logs for debugging
 
    process.wait()  # ✅ Ensure the process completes before exiting
 
    if process.returncode != 0:
        print(f"❌ FFmpeg exited with error code: {process.returncode}")
    else:
        print("✅ FFmpeg streaming successful!")
    # Store process ID
    processes[stream_id] = process
 
    # Prepare response data
    active_processes = [{"streamId": stream["id"], "title": stream["title"], "outputUrl": stream["url"]} for stream in streams]
 
    # 🔹 Save data to JSON
    save_stream_data({
        "date": data.get("date"),
        "scheduleType": data.get("scheduleType"),
        "time": data.get("time"),
        "sourceType": data.get("sourceType"),
        "selectedSource": input_url,
        "streams": active_processes
    })
 
    return jsonify({
        "message": "Streams started!",
        "is_streaming": True,
        "streams": active_processes
    })
@routes.route("/stop-stream", methods=["POST"])
def stop_stream():
    # Terminate all active streaming processes
    for stream_id, process in list(processes.items()):
        try:
            process.terminate()           # Attempt graceful termination
            process.wait(timeout=10)      # Wait up to 10 seconds for exit
        except Exception as e:
            process.kill()                # Force kill if needed
        # Remove the process from the dictionary
        processes.pop(stream_id, None)
    
    # Remove the persisted record from the JSON DB by overwriting the file with an empty list
    if os.path.exists(STREAM_DB_FILE):
        try:
            with open(STREAM_DB_FILE, "w") as file:
                json.dump([], file, indent=4)
        except Exception as e:
            return jsonify({"error": f"Failed to clear stream data: {e}"}), 500

    # Update streaming status
    stream_status["is_streaming"] = False
    return jsonify({"message": "Stream stopped", "is_streaming": False})

@routes.route("/get_stream_data", methods=["GET"])
def get_stream_data():
    if os.path.exists(STREAM_DB_FILE):
        try:
            with open(STREAM_DB_FILE, "r") as file:
                content = file.read().strip()  # Remove whitespace
                if not content:  # File is empty
                    return jsonify({}), 200
                try:
                    data = json.loads(content)
                except json.JSONDecodeError as e:
                    # app.logger.error("JSONDecodeError while parsing stream data: %s", e)
                    return jsonify({"error": "Invalid JSON data"}), 500

                # If data is a list, return the latest entry; adjust as needed
                if isinstance(data, list) and data:
                    return jsonify(data[-1]), 200
                # Otherwise, if data is a dict or empty list, return as is
                return jsonify(data), 200
        except Exception as e:
            app.logger.error("Error reading stream data: %s", e)
            return jsonify({"error": "Internal server error"}), 500
    else:
        return jsonify({"error": "Stream data not found"}), 404
@routes.route('/stream-status', methods=['GET'])
def get_stream_status():
    return jsonify({"is_streaming": stream_status["is_streaming"]})
# Serve files from the 'output_videos' directory
@routes.route("/output_videos/<path:filename>", methods=["GET"])
def serve_video(filename):
    try:
        return send_from_directory(OUTPUT_VIDEOS_DIR, filename)
    except FileNotFoundError:
        return {"error": "File not found"}, 404

@routes.route('/generate-presigned-url', methods=['POST'])
def generate_presigned_url_handler():
    try:
        file_name = request.json['file_name']
        #check in meta data db if file name already exists
        print('file_name:', file_name)  
        metadata = metadata_db.getByQuery({"file_name": file_name})
        print('metadata:', metadata)
        # Check if metadata array is not empty
        if metadata and metadata != []:
            return jsonify({'error': f'The filename {file_name} is already exists'}), 409
        
        
        full_file_name = f"{upload_video_folder}/{file_name}"
        presigned_url = generate_presigned_url_func(full_file_name)  # Call the correct function
        return jsonify({'url': presigned_url})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@routes.route('/add-metadata', methods=['POST'])
def add_metadata_api():
    try:
        # Retrieve metadata from the request body
        data = request.json
        file_name = data.get('file_name')
        bucket_name = BUCKET_NAME
        key= f"{upload_video_folder}/{file_name}"
        
        duration = get_video_duration_from_s3(bucket_name, key)

        # Validate input data
        if not file_name or not bucket_name:
            return jsonify({'error': 'Missing required fields'}), 400
        
        # Add metadata to the database
        result = add_metadata(key, bucket_name, duration )

        if result:
            return jsonify({'message': 'Metadata added successfully'}), 200
        else:
            return jsonify({'message': 'Metadata already exists'}), 409

    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
@routes.route('/schedule-video', methods=['POST'])
def schedule_video():
    try:
        # Retrieve the JSON data from the request
        data = request.json
        videos = data.get('videos', [])  # Array of video objects, default to empty list
        selected_date = data.get('selectedDate')  # Selected date in string format

        # If no selected date is provided, return error
        if not selected_date:
            return jsonify({'error': 'Selected date is required'}), 400

        # Convert the selected_date to a consistent format if it's in the long format
        try:
            if "GMT" in selected_date:
                # Parse the full date string and convert to YYYY-MM-DD format
                parsed_date = datetime.strptime(selected_date.split('GMT')[0].strip(), "%a %b %d %Y %H:%M:%S")
                selected_date = parsed_date.strftime("%Y-%m-%d")
        except Exception as date_error:
            print(f"Date parsing error: {date_error}")
            # If parsing fails, assume the date is already in correct format

        # Log received data for debugging
        print(f"Selected Date: {selected_date}")
        print(f"Videos: {videos}")

        # Initialize schedule_db with default structure
        schedule_db = {'data': []}

        # Try to read existing schedule from schedule_db.json
        try:
            with open('schedule_db.json', 'r') as f:
                file_content = f.read().strip()
                if file_content:  # Only try to load if file is not empty
                    schedule_db = json.loads(file_content)
        except FileNotFoundError:
            # File doesn't exist, we'll create it with default structure
            pass
        except json.JSONDecodeError:
            # File exists but is empty or invalid, use default structure
            pass

        # Ensure the structure of the schedule_db
        if 'data' not in schedule_db:
            schedule_db['data'] = []

        # Generate a random ID for new entries
        import random
        new_id = random.randint(100000000000000000, 999999999999999999)

        # If no videos are provided, remove the entire entry for the selected date
        if not videos:
            schedule_db['data'] = [entry for entry in schedule_db['data'] 
                                 if entry['date'] != selected_date]
        else:
            # Process videos if they exist
            schedule_found = False
            for entry in schedule_db['data']:
                if entry['date'] == selected_date:
                    # If the date matches, update the events
                    entry['events'] = []  # Clear existing events for this date
                    for video in videos:
                        start_time = datetime.fromisoformat(video['start_time'].replace('T', ' '))
                        end_time = start_time + timedelta(seconds=video['duration'])
                        entry['events'].append({
                            'target_id': video['target_id'],
                            'file_name': urllib.parse.unquote(video['file_name']),
                            'start_time': start_time.strftime("%Y-%m-%d %H:%M:%S"),
                            'end_time': end_time.strftime("%Y-%m-%d %H:%M:%S"),
                            'color': video.get('color', '#10b981'),
                        })
                    # Ensure ID exists
                    if 'id' not in entry:
                        entry['id'] = new_id
                    schedule_found = True
                    break

            # If no matching date is found and we have videos, add a new entry
            if not schedule_found and videos:
                new_entry = {
                    'date': selected_date,
                    'events': [],
                    'id': new_id
                }
                
                for video in videos:
                    start_time = datetime.fromisoformat(video['start_time'].replace('T', ' '))
                    end_time = start_time + timedelta(seconds=video['duration'])
                    new_entry['events'].append({
                        'target_id': video['target_id'],
                        'file_name': urllib.parse.unquote(video['file_name']),
                        'start_time': start_time.strftime("%Y-%m-%d %H:%M:%S"),
                        'end_time': end_time.strftime("%Y-%m-%d %H:%M:%S"),
                        'color': video.get('color', '#10b981'),
                    })
                schedule_db['data'].append(new_entry)

        # Sort the data array by date
        schedule_db['data'].sort(key=lambda x: datetime.strptime(x['date'], '%Y-%m-%d'))

        # Write the updated schedule back to schedule_db.json
        with open('schedule_db.json', 'w') as f:
            json.dump(schedule_db, f, indent=3)

        return jsonify({'message': 'Schedule updated successfully.'})

    except Exception as e:
        print(f"Error scheduling video: {e}")
        return jsonify({'error': str(e)}), 500
    


@routes.route('/fetch-metadata', methods=['GET'])
def fetch_metadata_json():
    try:
        # Example JSON data
        metadata_json = metadata_db.getAll()
        # update file_name 
        
        # add one more field bucket file url
        for data in metadata_json:
            data['file_url'] = f"https://{BUCKET_NAME}.s3.amazonaws.com/{data['file_name']}"
            file_name = os.path.basename(data['file_name'])
            data['file_name'] = os.path.splitext(file_name)[0]

        return jsonify(metadata_json), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
@routes.route('/fetch-scheduled-events', methods=['GET'])
def fetch_scheduled_events():
    try:
        # Fetch all scheduled events
        schedule = schedule_db.getAll()
        return schedule
    except Exception as e:
        print(f"Error fetching scheduled events: {e}")
        return []  
    

@routes.route('/start-ffmpeg-stream', methods=['GET'])
def start_ffmpeg_stream():
    try:
        currentDate = datetime.now(india_tz).date().isoformat()
        generate_event_file(currentDate)
        start_ffmpeg_service(currentDate)
        return jsonify({'message': 'FFmpeg stream started successfully'}), 200
    except Exception as e:
        print(f"Error starting FFmpeg stream: {e}")
        return None 