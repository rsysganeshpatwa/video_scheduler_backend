from datetime import datetime, timedelta
from ..utils import generate_event_file
import os
from app.ffmpeg_service import start_ffmpeg_service
import pytz

EVENT_FILE_DIR = 'event_files/'
india_tz = pytz.timezone('Asia/Kolkata')

def daily_task():
    date = datetime.now(india_tz).date().isoformat()

    # Delete previous day file
    previous_date = (datetime.now() - timedelta(days=1)).date().isoformat()
    previous_event_file = os.path.join(EVENT_FILE_DIR, f"{previous_date}.txt")
    if os.path.exists(previous_event_file):
        os.remove(previous_event_file)

    # Generate new event file and start stream
    generate_event_file(date)
    start_ffmpeg_service(date)
    
    print(f"Daily task completed for date {date}")
