import time
import os
import subprocess

# Configuration
PIPE_PATH = "/tmp/input.pipe"
BLANK_VIDEO = "blank.mp4"
PLAYLIST_FILE = "playlist.txt"
HLS_OUTPUT = "output.m3u8"
HLS_DIR = "./hls"

# Create necessary directories and files
def setup_environment():
    os.makedirs(HLS_DIR, exist_ok=True)
    # Create the named pipe only if it does not exist
    if not os.path.exists(PIPE_PATH):
        print("Creating named pipe...")
        os.mkfifo(PIPE_PATH)
    if not os.path.exists(BLANK_VIDEO):
        print("Generating blank video...")
        subprocess.run([
            "ffmpeg", "-f", "lavfi", "-i", "color=c=black:s=1280x720:d=360",
            "-vf", "drawtext=text='No Scheduled Content':fontcolor=white:fontsize=50:x=(w-text_w)/2:y=(h-text_h)/2",
            "-c:v", "libx264", "-t", "10", BLANK_VIDEO
        ])

# Read schedule from playlist file
def read_schedule(file_path):
    schedule = {}
    if os.path.exists(file_path):
        with open(file_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line:
                    time_str, video_file = line.split(maxsplit=1)
                    print(f"Scheduling {video_file} at {time_str}")
                    schedule[time_str] = video_file
    return schedule

# Get current time in HH:MM format
def get_current_time():
    return time.strftime("%H:%M")

# Start FFmpeg to generate HLS output (this happens only once)
def start_ffmpeg():
    print("Starting FFmpeg for HLS streaming...")
    # FFmpeg will open the pipe once and stream continuously
    ffmpeg_command = [
        "ffmpeg", "-re", "-i", PIPE_PATH,
        "-c:v", "libx264", "-preset", "veryfast", "-b:v", "1500k",
        "-c:a", "aac", "-b:a", "128k",
        "-f", "hls", "-hls_time", "6", "-hls_list_size", "5",
        "-hls_playlist_type", "event",  # Use 'event' type for continuous live stream
        "-hls_flags", "delete_segments",
        f"{HLS_DIR}/{HLS_OUTPUT}"
    ]
    
    # Execute the FFmpeg process
    subprocess.Popen(ffmpeg_command)

# Ensure the pipe is created and available, avoiding the overwrite prompt
def write_to_pipe(video_file):
    print(f"Streaming {video_file} to the pipe...")

    # Check if the pipe exists; if not, create it
    if not os.path.exists(PIPE_PATH):
        print("Named pipe does not exist, creating it...")
        os.mkfifo(PIPE_PATH)

    # Use subprocess.Popen to write to the pipe without reopening FFmpeg
    # Run this command in the background continuously
    try:
        ffmpeg_process = subprocess.Popen([
            "ffmpeg", "-y","-re", "-i", video_file, "-c:v", "copy", "-c:a", "copy",
            "-f", "mpegts", PIPE_PATH
        ], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        # Handle FFmpeg errors or warnings if needed
     

    except Exception as e:
        print(f"Error occurred while streaming to pipe: {str(e)}")
# Main function to manage the stream
def main():
    setup_environment()  # Create pipe and blank video if needed
    start_ffmpeg()  # Start FFmpeg only once
  
    processed_times = set()

    while True:
        current_time = get_current_time()
        schedule = read_schedule(PLAYLIST_FILE)

        # Check if there's a video scheduled for the current time
        if current_time in schedule :
            video_file = schedule[current_time]
            if os.path.exists(video_file):
                processed_times.add(current_time)
            else:
                print(f"Scheduled video {video_file} not found. Using blank video.")
                video_file = BLANK_VIDEO
        else:
            # No scheduled content, use blank video
            video_file = BLANK_VIDEO

        # Write video to the pipe
        if os.path.exists(PIPE_PATH):
            write_to_pipe(video_file)
        else:
            print("Named pipe not found. Exiting.")
            break

        # Wait a while before checking the schedule again
        time.sleep(10)

if __name__ == "__main__":
    main()
