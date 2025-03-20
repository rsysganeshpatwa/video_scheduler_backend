import requests
from flask import jsonify
import os
from .hls_service import start_fetcher

FFMPEG_SERVICE_URL = "http://video_ffmpeg_service:5001/ffmpeg/start"

def start_ffmpeg_service(current_date):
    try:
       
        event_file = os.path.abspath(f"event_files/{current_date}.txt")  # Absolute path
        output_video_dir = os.path.abspath("output_videos")  # Absolute
        


        # Data to send to the FFmpeg service
        data = {
            "date": current_date,
            "event_file": event_file,
            "output_video_dir": output_video_dir
        }

        # Make a request to the FFmpeg service
        response = requests.post(FFMPEG_SERVICE_URL, json=data)

        if response.status_code == 200:
            #start_fetcher()
            return jsonify({'message': 'FFmpeg stream started successfully'}), 200
        else:
            return jsonify({'error': 'Failed to start FFmpeg stream', 'details': response.json()}), response.status_code
    except Exception as e:
        print(f"Error starting FFmpeg stream: {e}")
        return jsonify({'error': 'Internal server error', 'details': str(e)}), 500
