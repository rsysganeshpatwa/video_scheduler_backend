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

routes = Blueprint('routes', __name__)


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
        # Retrieve the array of video schedule data from the request
        data = request.json
        videos = data['videos']  # Array of video objects, each containing 'file_name', 'start_time', and 'duration'

          # Clear the schedule_db before starting the update and initialize the response object
        schedule_db.deleteAll()
         
        # Clear the schedule_db before starting the update
        
    
        # Initialize the response object
        message = "All videos scheduled successfully."

        # Iterate over each video in the array
        for video in videos:
            file_name = video['file_name']
            start_time = datetime.fromisoformat(video['start_time'])
            duration = video['duration']  # Duration in seconds
           
            # Fetch metadata for the video
            file_name = urllib.parse.unquote(file_name)
            metadata = metadata_db.getByQuery({"file_name": file_name})
            if not metadata:
                message = f"Error scheduling {file_name}: Video metadata not found."
                
                break

            # Update schedule database
            date_str = start_time.date().isoformat()

            # Query to get existing schedule for the date
            schedule_data = schedule_db.getByQuery({"date": date_str})

               # Handle different cases for schedule_data
            if not schedule_data:
                # No existing schedule for this date, create a new one
                schedule = {"date": date_str, "events": []}
                schedule_db.add(schedule)
            elif isinstance(schedule_data, list) and len(schedule_data) > 0:
                # If it's a list, take the first matching schedule
                schedule = schedule_data[0]
            elif isinstance(schedule_data, dict) and "data" in schedule_data:
                # If it's a dictionary, access the "data" field
                schedule = schedule_data["data"]
            else:
                # Handle unexpected structure
                print(f"Error scheduling {file_name}: Invalid schedule data structure.")
                message = f"Error scheduling {file_name}: Invalid schedule data structure."
                break
        
            print(f"Schedule for {file_name} {date_str}: {schedule}")
           


            # Check if the file is already scheduled for the specified time
            for event in schedule['events']:
                if event['file_name'] == file_name and event['start_time'] == str(start_time):
                    print(f"Video {file_name} is already scheduled for {start_time}.")
                    break
            else:
                # Create a new event if no matching event was found
                end_time = start_time + timedelta(seconds=duration)
                schedule['events'].append({
                    'file_name': file_name,
                    'start_time': str(start_time),
                    'end_time': str(end_time)
                })
                # Update the schedule in the database
                schedule_db.updateByQuery({"date": date_str}, {"events": schedule['events']})

                # Update event file (optional based on your application logic)
                generate_event_file(date_str)
                # Start the stream if date is today
               

        return jsonify({'message': message})

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
        currentDate = datetime.now().date().isoformat()
        generate_event_file(currentDate)
        start_ffmpeg_service(currentDate)
        return jsonify({'message': 'FFmpeg stream started successfully'}), 200
    except Exception as e:
        print(f"Error starting FFmpeg stream: {e}")
        return None 