import os
from datetime import datetime
import pytz
from app.config import BUCKET_NAME, BLANK_VIDEO_PATH, EVENT_FILE_DIR

class MockDatabase:
    def __init__(self):
        self.data = {
            '2025-01-15': {
                'events': [
                    {'start_time': '2025-01-15T10:00:00', 'end_time': '2025-01-15T11:00:00', 'file_name': 'event1.mp4'},
                    {'start_time': '2025-01-15T13:00:00', 'end_time': '2025-01-15T14:00:00', 'file_name': 'event2.mp4'},
                    {'start_time': '2025-01-15T22:30:00', 'end_time': '2025-01-15T22:00:00', 'file_name': 'event3.mp4'}
                ]
            }
        }

    def getByQuery(self, query):
        # This is a simplified mock query, which returns the events for a given date
        date = query.get("date")
        if date in self.data:
            return [{ 'date': date, 'events': self.data[date]['events'] }]
        return []

# Create an instance of the mock database
schedule_db = MockDatabase()


# Assuming you want to work with your local server time or a specific timezone
LOCAL_TIMEZONE = pytz.timezone('Asia/Kolkata')  # Replace with your desired timezone

def generate_event_file(date):
    try:
        # Fetch schedule for the given date
        schedule = schedule_db.getByQuery({"date": date})
        events = schedule[0]['events'] if schedule else []

        lines = []
        line_number = 1  # Initialize line number

        # Get the current time in the specified timezone and format
        now = datetime.now(LOCAL_TIMEZONE)
        current_time_24hr = now.strftime('%Y-%m-%d %H:%M:%S')
        current_time = datetime.strptime(current_time_24hr, '%Y-%m-%d %H:%M:%S')
        current_time = LOCAL_TIMEZONE.localize(current_time)
        print(f"Current Time: {current_time}")

        for event in sorted(events, key=lambda e: e['start_time']):
            start_time = datetime.fromisoformat(event['start_time'])
            end_time = datetime.fromisoformat(event['end_time'])

            # Localize event times to the same timezone
            start_time = LOCAL_TIMEZONE.localize(start_time)
            end_time = LOCAL_TIMEZONE.localize(end_time)

            # Skip event if it's already in the past
            if start_time < current_time:
                print(f"Skipping event {event['file_name']} as it is in the past ({start_time})")
                continue  # Skip the event if its start time is in the past

            # Fill gap with blank videos if current time is before the event's start time
            if current_time < start_time:
                gap_duration = (start_time - current_time).total_seconds()
                full_blanks = int(gap_duration // 1)  # Number of 1-second blanks

                # Append full blank video repetitions
                for _ in range(full_blanks):
                    lines.append(f"file '{BLANK_VIDEO_PATH}'")
                    lines.append("duration 1")
                    line_number += 2  # Each blank video adds 2 lines (file + duration)

            # Append the event video
            file_path = f"https://{BUCKET_NAME}.s3.amazonaws.com/{event['file_name']}"
            lines.append(f"file '{file_path}'")
            duration = (end_time - start_time).total_seconds()
            lines.append(f"duration {duration}")

            print(f"Added event video: {file_path} at line {line_number}, current time: {current_time}")
            line_number += 2  # Event video adds 2 lines (file + duration)

            # Update current time to the end of the event
            current_time = end_time

        # Fill remaining time to the end of the day (23:59:59) with blank videos
        end_of_day = datetime.combine(datetime.fromisoformat(date), datetime.max.time())
        end_of_day = LOCAL_TIMEZONE.localize(end_of_day)
        print(f"End of Day: {end_of_day}")

        if current_time < end_of_day:
            remaining_duration = (end_of_day - current_time).total_seconds()
            full_blanks = int(remaining_duration // 1)

            # Append full blank video repetitions
            for _ in range(full_blanks):
                lines.append(f"file '{BLANK_VIDEO_PATH}'")
                lines.append("duration 1")
                line_number += 2  # Each blank video adds 2 lines (file + duration)

        # Write the event file
        event_file_path = os.path.join(EVENT_FILE_DIR, f"{date}.txt")
        with open(event_file_path, 'w') as f:
            f.write('\n'.join(lines))

        print(f"Event file generated successfully: {event_file_path}")
    except Exception as e:
        print(f"Error generating event file: {e}")


# Test the function
generate_event_file('2025-01-15')
