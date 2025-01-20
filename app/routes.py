from flask import Blueprint, request, jsonify
from datetime import datetime, timedelta
from .databases import metadata_db, schedule_db
from .utils import generate_event_file, get_video_duration_from_s3, generate_presigned_url_func, add_metadata
from .config import s3_client, BUCKET_NAME,upload_video_folder
import ffmpeg
import os

routes = Blueprint('routes', __name__)

# Define default route

@routes.route('/')
def home():
    return "Welcome to the Video Scheduler API! The server is running."

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
        data = request.json
        file_name = data['file_name']
        start_time = datetime.fromisoformat(data['start_time'])

        # Fetch metadata
        metadata = metadata_db.getByQuery({"file_name": file_name})
        if not metadata:
            return jsonify({'error': 'Video metadata not found'}), 404

        print(f"Metadata for {file_name} already exists.")

        # Update schedule database
        date_str = start_time.date().isoformat()

        # Query to get existing schedule for the date
        schedule_data = schedule_db.getByQuery({"date": date_str})
        print('schedule_data:---------', schedule_data)
# If schedule_data is a list, we iterate to find the entry for the given date
        if isinstance(schedule_data, list):
            matching_entry = next((entry for entry in schedule_data if entry.get('date') == date_str), None)
            
            if matching_entry:
                print(f"Found schedule for date {date_str}")
                # If events are empty, initialize with an empty event list
                if not matching_entry['events']:
                    print(f"Events are empty for date {date_str}, initializing.")
                    matching_entry['events'] = []
            else:
                print(f"No schedule found for date {date_str}, initializing with empty events.")
                # Add a new entry for this date with empty events if not found
                schedule_db.add({"date": date_str, "events": []})

        # Check if the schedule data is a list or a dictionary
        if isinstance(schedule_data, list) and schedule_data:
            # If it's a list, get the first item
            schedule = schedule_data[0]
        elif isinstance(schedule_data, dict) and 'data' in schedule_data:
            # If it's a dictionary, access the 'data' field
            schedule = schedule_data['data']
        else:
            # Handle unexpected structure (e.g., neither a list nor a dict with 'data')
            return jsonify({'error': 'Invalid schedule data structure'}), 500

        # Check if the file is already scheduled for the specified time
        for event in schedule['events']:
            if  event['start_time'] == str(start_time):
                return jsonify({'error': 'Video already scheduled for the specified time'}), 409

        # Generate presigned URL for the video
        url = s3_client.generate_presigned_url('get_object',
                                               Params={'Bucket': BUCKET_NAME, 'Key': file_name},
                                               ExpiresIn=3600)  # URL valid for 1 hour

        # Use ffmpeg to probe the video stream directly from the S3 URL
        print(f"Probing video at URL: {url}")
        probe = ffmpeg.probe(url, v='error', select_streams='v:0', show_entries='stream=duration')
        print(f"Probe result: {probe}")

        # Extract the duration in seconds
        duration = float(probe['streams'][0]['duration'])
        end_time = start_time + timedelta(seconds=duration)

        # Add the new event to the schedule
        schedule['events'].append({
            'file_name': file_name,
            'start_time': str(start_time),
            'end_time': str(end_time)
        })

        # Update the schedule in the database
        schedule_db.updateByQuery({"date": date_str}, {"events": schedule['events']})
        print(f"Video scheduled for {start_time} - {end_time}")

        # Update event file
        generate_event_file(date_str)

        return jsonify({'message': 'Scheduled successfully'})
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