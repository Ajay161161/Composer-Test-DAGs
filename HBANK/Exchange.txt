import base64
from google.cloud import storage

def decode_b64_to_csv(bucket_name, file_path):

    try:

        client = storage.Client()

        bucket = client.bucket(bucket_name)

        blob = bucket.blob(file_path)

        print(f"Reading file: {file_path}")

        encoded_data = blob.download_as_text()

        decoded_data = base64.b64decode(encoded_data)

        blob.upload_from_string(
            decoded_data,
            content_type='text/csv'
        )

        print(f"Successfully decoded file: {file_path}")

        return True

    except Exception as e:

        print(f"Failed decoding file {file_path}: {str(e)}")

        raise
