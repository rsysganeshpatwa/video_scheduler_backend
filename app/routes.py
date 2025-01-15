from flask import Blueprint, request, jsonify
from datetime import datetime, timedelta
from .databases import metadata_db, schedule_db
from .utils import generate_event_file, get_video_duration_from_s3, generate_presigned_url


routes = Blueprint('routes', __name__)

@routes.route('/generate-presigned-url', methods=['POST'])
def generate_presigned_url():
    try:
        file_name = request.json['file_name']
        presigned_url = generate_presigned_url(file_name)
        return jsonify({'url': presigned_url})
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

        duration = get_video_duration_from_s3(metadata['bucket_name'], file_name)
        end_time = start_time + timedelta(seconds=duration)

        # Update schedule database
        date_str = start_time.date().isoformat()
        schedule = schedule_db.getByQuery({"date": date_str})

        if not schedule:
            schedule_db.add({"date": date_str, "events": []})
            schedule = schedule_db.getByQuery({"date": date_str})[0]

        schedule['events'].append({
            'file_name': file_name,
            'start_time': str(start_time),
            'end_time': str(end_time)
        })
        schedule_db.updateByQuery({"date": date_str}, {"events": schedule['events']})

        # Update event file
        generate_event_file(date_str)

        return jsonify({'message': 'Scheduled successfully'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500
