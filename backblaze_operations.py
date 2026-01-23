import boto3
import os
from botocore.exceptions import NoCredentialsError, ClientError


def upload_file(file_path, ENDPOINT_URL, KEY_ID, APPLICATION_KEY, BUCKET_NAME, object_name=None):
    """
    Upload a file to an S3 compatible bucket (Backblaze B2)

    :param file_path: File to upload
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """
    
    # Check for configuration
    if not all([ENDPOINT_URL, KEY_ID, APPLICATION_KEY, BUCKET_NAME]):
        print("Error: Missing B2 configuration.")
        print("Please ensure B2_ENDPOINT_URL, B2_KEY_ID, B2_APPLICATION_KEY, and B2_BUCKET_NAME are set in your .env file.")
        return False

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_path)

    # Initialize the S3 client
    s3_client = boto3.client(
        's3',
        endpoint_url=ENDPOINT_URL,
        aws_access_key_id=KEY_ID,
        aws_secret_access_key=APPLICATION_KEY
    )

    try:
        #Check if file already exists
        try: 
            s3_client.head_object(Bucket=BUCKET_NAME, Key=object_name)
            print(f"File {object_name} already exists in bucket {BUCKET_NAME}. Skipping upload.")
            return True
        except ClientError:
            # File not found, proceed with upload
            pass

        print(f"Starting upload: {file_path} -> {BUCKET_NAME}/{object_name}")
        s3_client.upload_file(file_path, BUCKET_NAME, object_name)
        print(f"Upload Successful: {object_name}")
        return True
    except FileNotFoundError:
        print(f"The file {file_path} was not found for Backblaze upload")
    except NoCredentialsError:
        print("Backblaze redentials not available")
    except Exception as e:
        print(f"An error with Backblaze occurred: {e}")
