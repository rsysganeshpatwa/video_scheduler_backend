import random
from datetime import datetime, timedelta

def generate_random_color():
    """Generate a random hex color."""
    return f"#{random.randint(0, 255):02X}{random.randint(0, 255):02X}{random.randint(0, 255):02X}"

def generate_unique_numeric_id():
    """Generate a unique 18-digit numeric ID."""
    return random.randint(10**17, 10**18 - 1)

def schedule_events(data, schedule_start_time, schedule_end_time):
    # Parse the start and end times
    schedule_start_time = datetime.strptime(schedule_start_time, '%Y-%m-%d %H:%M:%S')
    schedule_end_time = datetime.strptime(schedule_end_time, '%Y-%m-%d %H:%M:%S')

    # Total duration of all events
    total_duration = sum(event['duration'] for event in data)

    if (schedule_end_time - schedule_start_time).total_seconds() < total_duration:
        raise ValueError("The provided time range is too short for all events.")

    # Scheduling events
    current_time = schedule_start_time
    scheduled_events = []

    while current_time <= schedule_end_time:
        for event in data:
            # If the next event exceeds the end time, we stop
            if current_time + timedelta(seconds=event['duration']) > schedule_end_time:
                current_time = schedule_end_time
                break

            event_start_time = current_time + timedelta(seconds=1)  # Add 1 second to the current time
            event_end_time = event_start_time + timedelta(seconds=event['duration'])

            scheduled_events.append({
                "target_id": generate_unique_numeric_id(),
                "file_name": event['file_name'],
                "start_time": event_start_time.strftime('%Y-%m-%d %H:%M:%S'),
                "end_time": event_end_time.strftime('%Y-%m-%d %H:%M:%S'),
                "color": generate_random_color()
            })

            current_time = event_end_time

        # If we have reached the end time, break out of the while loop
        if current_time >= schedule_end_time:
            break

    # Prepare the final structure
    output = {
        "data": [{
            "date": schedule_start_time.strftime('%Y-%m-%d'),
            "events": scheduled_events,
            "id": generate_unique_numeric_id()  # Unique ID for the day
        }]
    }

    return output

# Example usage
data = [
    {
        "file_name": "content-scheduler/uploaded_videos/Interview_With_rsystem.mp4",
        "bucket_name": "pocrsibucket",
        "duration": 768.0673
    },
    {
        "file_name": "content-scheduler/uploaded_videos/bhoomi meeting.mp4",
        "bucket_name": "pocrsibucket",
        "duration": 382.849
    },
    {
        "file_name": "content-scheduler/uploaded_videos/AI_Nasscom_DES.mp4",
        "bucket_name": "pocrsibucket",
        "duration": 275.408733
    },
    {
        "file_name": "content-scheduler/uploaded_videos/Nasscom_DES.mp4",
        "bucket_name": "pocrsibucket",
        "duration": 1167.400733
    }
]

schedule_start_time = "2025-01-30 09:00:00"
schedule_end_time = "2025-01-30 21:00:00"

try:
    scheduled_events = schedule_events(data, schedule_start_time, schedule_end_time)
    print(scheduled_events)
except ValueError as e:
    print(e)
