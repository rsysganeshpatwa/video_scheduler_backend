from apscheduler.schedulers.background import BackgroundScheduler
from .tasks.daily_task import daily_task
from .config import EVENT_FILE_DIR
from datetime import datetime
import os
from .utils import start_stream

def initialize_scheduler(app):
    
    scheduler = BackgroundScheduler()
 # Delete the previous day's file
    todayFileDate = (datetime.now()).date().isoformat()
    today_event_file = os.path.join(EVENT_FILE_DIR, f"{todayFileDate}.txt")
    
    if not os.path.exists(today_event_file):
        print(f"Event file not found for date {todayFileDate}")
        daily_task()


    scheduler.add_job(daily_task, 'cron', hour=0, minute=0)  # Runs daily at midnight
    scheduler.start()

   

    # Graceful shutdown of scheduler
    @app.teardown_appcontext
    def shutdown_scheduler(exception=None):
        scheduler.shutdown()
