from app import create_app
from app.config import EVENT_FILE_DIR, OUTPUT_VIDEO_DIR
import os

app = create_app()

# Ensure event files directory exists
os.makedirs(EVENT_FILE_DIR, exist_ok=True)
os.makedirs(OUTPUT_VIDEO_DIR, exist_ok=True)
if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
    print("server started")
