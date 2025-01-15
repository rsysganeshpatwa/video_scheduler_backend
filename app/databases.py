from pysondb import db

metadata_db = db.getDb("metadata_db.json")
schedule_db = db.getDb("schedule_db.json")

def add_metadata(file_name, bucket_name, duration):
    try:
        # Check if the file already exists in the database
        existing_metadata = metadata_db.getByQuery({"file_name": file_name})
        if existing_metadata:
            print(f"Metadata for {file_name} already exists.")
            return False

        # Add the metadata to the database
        metadata_db.add({
            "file_name": file_name,
            "bucket_name": bucket_name,
            "duration": duration
        })
        print(f"Metadata for {file_name} added successfully.")
        return True
    except Exception as e:
        print(f"Error adding metadata: {e}")
        return False

