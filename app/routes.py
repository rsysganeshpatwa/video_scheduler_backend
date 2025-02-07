from flask import Blueprint, request, jsonify,send_from_directory
from datetime import datetime, timedelta
from .databases import metadata_db, schedule_db
from .utils import generate_event_file, get_video_duration_from_s3, generate_presigned_url_func, add_metadata
from .config import s3_client, BUCKET_NAME,upload_video_folder
import ffmpeg
import os
from  .stream_handler import start_stream
import urllib.parse
from .ffmpeg_service import start_ffmpeg_service
import json
import pytz

routes = Blueprint('routes', __name__)

# Define the India time zone
india_tz = pytz.timezone('Asia/Kolkata')
# Path to the 'output_videos' directory
OUTPUT_VIDEOS_DIR = os.path.join(os.getcwd(), "output_videos")

# Ensure the directory exists
if not os.path.exists(OUTPUT_VIDEOS_DIR):
    os.makedirs(OUTPUT_VIDEOS_DIR)

# Define default route

@routes.route('/')
def home():
    return "Welcome to the Video Scheduler API! The server is running."

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