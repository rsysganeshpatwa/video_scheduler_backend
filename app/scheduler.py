from apscheduler.schedulers.background import BackgroundScheduler
from .tasks.daily_task import daily_task
from .config import EVENT_FILE_DIR
from datetime import datetime
import os
from flask import current_app
import threading

# Flag to check if the scheduler has already been initialized
scheduler_initialized = False

def initialize_scheduler(app):
    global scheduler_initialized
    
    # If scheduler is already initialized, skip initialization
    if scheduler_initialized:
        print("Scheduler already initialized.")
        return

    def start_scheduler():
        scheduler = BackgroundScheduler()

        def scheduled_daily_task():
            with app.app_context():
                daily_task()

        # Delete the previous day's file and check for today's file
        todayFileDate = datetime.now().date().isoformat()
        today_event_file = os.path.join(EVENT_FILE_DIR, f"{todayFileDate}.txt")

        if os.path.exists(today_event_file):
            print(f"Previous day's event file deleted for date {todayFileDate}")
            os.remove(today_event_file)

        # Add the daily task job
        scheduler.add_job(scheduled_daily_task, 'cron', hour=0, minute=0)  # Adjust the time as needed
        print("Job added")
        scheduler.start()
        print("Scheduler started")

    # Start the scheduler in a separate thread
    scheduler_thread = threading.Thread(target=start_scheduler)
    scheduler_thread.daemon = True  # This ensures the thread will exit when the main program exits
    scheduler_thread.start()

    # Mark the scheduler as initialized
    scheduler_initialized = True
