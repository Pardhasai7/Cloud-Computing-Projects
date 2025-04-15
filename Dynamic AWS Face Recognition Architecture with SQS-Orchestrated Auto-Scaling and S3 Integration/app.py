import os
import json
import boto3
import base64
import subprocess
import time
from flask import Flask
from botocore.exceptions import ClientError

# Initialize Flask app
app = Flask(__name__)

# AWS Resource configuration
REQUEST_QUEUE_URL = "1229568589-req-queue"
RESPONSE_QUEUE_URL = "1229568589-resp-queue"
INPUT_BUCKET_NAME = "1229568589-in-bucket"
OUTPUT_BUCKET_NAME = "1229568589-out-bucket"

# Initialize AWS clients
sqs_client = boto3.resource('sqs', region_name='us-east-1')
s3_client = boto3.resource('s3', region_name='us-east-1')
request_queue = sqs_client.Queue(REQUEST_QUEUE_URL)
response_queue = sqs_client.Queue(RESPONSE_QUEUE_URL)

# Define paths for directories and create them if necessary
home_directory = os.path.expanduser("~")
model_directory = os.path.join(home_directory, "model")
app_directory = os.path.join(home_directory, "app")
temp_directory = os.path.join(app_directory, "temp")

os.makedirs(temp_directory, exist_ok=True)

# Function to execute a subprocess and run the face recognition Python script
def execute_face_recognition(script_location, image_location):
    try:
        process_result = subprocess.run(
            ['python3', script_location, image_location],
            capture_output=True, text=True
        )
        if process_result.returncode != 0:
            print(f"Error in running script: {process_result.stderr}")
            return None
        return process_result.stdout.strip()
    except Exception as exec_error:
        print(f"Execution failed: {exec_error}")
        return None

# Function to handle a single message from the SQS request queue
def handle_sqs_message(sqs_message):
    try:
        message_content = json.loads(sqs_message.body)
        image_filename = message_content['fileName']
        image_content = base64.b64decode(message_content['imageData'])

        # Create a temporary image file from base64 data
        temp_image_path = os.path.join(temp_directory, image_filename)
        with open(temp_image_path, 'wb') as temp_image_file:
            temp_image_file.write(image_content)

        # Run the face recognition script
        script_path = os.path.join(model_directory, "face_recognition.py")
        recognition_output = execute_face_recognition(script_path, temp_image_path)

        if recognition_output:
            # Upload recognition result to S3 output bucket
            result_file_key = os.path.splitext(image_filename)[0]
            s3_client.Bucket(OUTPUT_BUCKET_NAME).put_object(
                Key=f"{result_file_key}.txt", Body=recognition_output
            )

            # Send the result to the response SQS queue
            response_content = {
                "fileName": f"{result_file_key}.jpg",
                "result": recognition_output
            }
            response_queue.send_message(MessageBody=json.dumps(response_content))

            # Delete the message from the request queue
            sqs_message.delete()

            # Remove temporary image file
            os.remove(temp_image_path)

    except ClientError as client_error:
        print(f"ClientError: {client_error}")
    except Exception as error:
        print(f"Error: {error}")

# Function to continuously poll the SQS queue for new messages
def poll_request_queue():
    while True:
        try:
            # Poll the SQS request queue for new messages
            received_messages = request_queue.receive_messages(
                MaxNumberOfMessages=1, WaitTimeSeconds=19
            )

            if received_messages:
                handle_sqs_message(received_messages[0])
            else:
                print("No new messages. Waiting...")
                time.sleep(1)

        except ClientError as client_error:
            print(f"ClientError while receiving message: {client_error}")
            time.sleep(5)

# Start polling the request queue for messages
poll_request_queue()
