from apscheduler.schedulers.background import BackgroundScheduler
from .tasks.daily_task import daily_task

def initialize_scheduler(app):
    scheduler = BackgroundScheduler()
    scheduler.add_job(daily_task, 'cron', hour=0, minute=0)  # Runs daily at midnight
    scheduler.start()

    # Graceful shutdown of scheduler
    @app.teardown_appcontext
    def shutdown_scheduler(exception=None):
        scheduler.shutdown()
