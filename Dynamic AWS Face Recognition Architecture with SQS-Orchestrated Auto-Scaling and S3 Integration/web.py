import os
import json
import base64
import time
import asyncio
import boto3
from flask import Flask, request, jsonify
from botocore.exceptions import ClientError
from threading import Thread

app = Flask(__name__)

# AWS Resource Setup
INPUT_BUCKET = "1229568589-in-bucket"
OUTPUT_BUCKET = "1229568589-out-bucket"
REQUEST_QUEUE_URL = "1229568589-req-queue"
RESPONSE_QUEUE_URL = "1229568589-resp-queue"

INSTANCE_IDS = [
    'i-0b49ae20b7dc4278b', 'i-05217a5f8e90d0094', 'i-098f59a3ba906cdd3',
    'i-0cf71971b0fd6c220', 'i-07d02956c9f946e17', 'i-0c2335872af9fe922',
    'i-0332606f21f8c19f2', 'i-08fd6620d367470ca', 'i-0a09f8a7e138ba1a2',
    'i-0f298c7e84703d7da', 'i-005f6b8082873d0fa', 'i-0544c1ec0af462ce4',
    'i-05d190b5f2ec567d0', 'i-04bbb7e213a0fda9f', 'i-08c26a08b4c2ca044',
    'i-056d5592c5ca465a2', 'i-01846be5a4934cadd', 'i-073ee009f0cd2607b',
    'i-027a392cc6523a45e', 'i-0e871ba7e468472a8'
]

# Initialize Boto3 Resources
ec2 = boto3.resource('ec2', region_name='us-east-1')
sqs = boto3.resource('sqs', region_name='us-east-1')
s3 = boto3.resource('s3', region_name='us-east-1')

pending_tasks = {}

# Upload file to S3
def upload_to_s3(bucket, file_content, file_name):
    try:
        s3.Bucket(bucket).put_object(Key=file_name, Body=file_content)
        print(f"Uploaded {file_name} to {bucket}")
        return True
    except ClientError as e:
        print(f"Error uploading to S3: {e}")
        return False

# Send message to SQS
def send_to_sqs(queue, file_name, file_content):
    try:
        message = json.dumps({
            'fileName': file_name,
            'imageData': base64.b64encode(file_content).decode('utf-8')
        })
        queue.send_message(MessageBody=message)
        return True
    except ClientError as e:
        print(f"Error sending message to SQS: {e}")
        return False

# Wait for classification response
async def get_classification_result(file_name):
    while file_name not in pending_tasks:
        await asyncio.sleep(1)

    response_queue = sqs.get_queue_by_name(QueueName=RESPONSE_QUEUE_URL)
    messages = response_queue.receive_messages(MaxNumberOfMessages=10, WaitTimeSeconds=20)

    for msg in messages:
        body = json.loads(msg.body)
        if body.get('fileName') == file_name:
            pending_tasks[file_name] = body.get('result')
            msg.delete()
            break

# Adjust EC2 instances based on workload
def adjust_ec2_instances(required_instances):
    instance_states = check_ec2_instances()
    running = len(instance_states['running'])
    needed = required_instances - running

    if needed > 0:
        start_instances(instance_states['stopped'][:needed])
    elif needed < 0:
        stop_instances(instance_states['running'][:abs(needed)])

# Fetch the number of messages in the request queue
def get_queue_length():
    try:
        queue = sqs.get_queue_by_name(QueueName=REQUEST_QUEUE_URL)
        return int(queue.attributes.get('ApproximateNumberOfMessages', 0))
    except ClientError as e:
        print(f"Error getting queue length: {e}")
        return 0

# Get the current state of EC2 instances
def check_ec2_instances():
    running_instances, stopped_instances = [], []
    instances = ec2.instances.filter(InstanceIds=INSTANCE_IDS)

    for instance in instances:
        if instance.state['Name'] in ['running', 'pending']:
            running_instances.append(instance.id)
        elif instance.state['Name'] == 'stopped':
            stopped_instances.append(instance.id)

    return {
        'running': running_instances,
        'stopped': stopped_instances
    }

# Start EC2 instances
def start_instances(instance_ids):
    ec2.instances.filter(InstanceIds=instance_ids).start()
    print(f"Starting instances: {instance_ids}")

# Stop EC2 instances
def stop_instances(instance_ids):
    ec2.instances.filter(InstanceIds=instance_ids).stop()
    print(f"Stopping instances: {instance_ids}")

# Adjust EC2 instances periodically based on queue length
def ec2_monitor():
    while True:
        queue_length = get_queue_length()
        required_instances = min(20, max(1, queue_length // 4))
        adjust_ec2_instances(required_instances)
        time.sleep(5)

# Route to upload file and process it
@app.route("/", methods=["POST"])
async def handle_upload():
    if 'inputFile' not in request.files:
        return jsonify({"error": "No file uploaded"}), 400

    file = request.files['inputFile']
    file_name = file.filename
    file_content = file.read()

    req_queue = sqs.get_queue_by_name(QueueName=REQUEST_QUEUE_URL)
    if not send_to_sqs(req_queue, file_name, file_content):
        return jsonify({"error": "Failed to send message to SQS"}), 500

    if not upload_to_s3(INPUT_BUCKET, file_content, file_name):
        return jsonify({"error": "Failed to upload file to S3"}), 500

    await get_classification_result(file_name)

    result = pending_tasks.pop(file_name, None)
    if result:
        return jsonify({file_name: result}), 200
    else:
        return jsonify({"error": "Failed to process file"}), 500

# Start the EC2 instance monitor
if __name__ == "__main__":
    monitor_thread = Thread(target=ec2_monitor)
    monitor_thread.daemon = True
    monitor_thread.start()

    app.run(host='0.0.0.0', port=3000)
