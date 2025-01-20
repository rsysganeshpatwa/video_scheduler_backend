import os
from datetime import datetime, timedelta

# Define your video file
video_file = "example_video.mp4"

# Get the current time
current_time = datetime.now()

# Round the current time to the nearest full minute (set seconds and microseconds to 0)
current_time = current_time.replace(second=0, microsecond=0)

# Set the number of intervals you want (e.g., 10 intervals)
num_intervals = 10

# Create the playlist
playlist = []
for i in range(num_intervals):
    # Add a 2-minute interval to the current time
    interval_time = current_time + timedelta(minutes=i*2)
    formatted_time = interval_time.strftime("%H:%M")
    playlist.append(f"{formatted_time} {video_file}")

# Print the playlist
for entry in playlist:
    print(entry)
