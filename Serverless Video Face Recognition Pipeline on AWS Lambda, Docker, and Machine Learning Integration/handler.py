import boto3
import os
import subprocess
import logging
import json

s3_client = boto3.client('s3')
lambda_client = boto3.client('lambda')

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def process_video(input_file, output_file):
    cmd = f"ffmpeg -i '{input_file}' -vframes 1 '{output_file}'"
    logger.info(f"Running command: {cmd}")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        logger.error(f"ffmpeg command failed: {result.stderr}")
        raise Exception(f"ffmpeg command failed: {result.stderr}")
    logger.info(f"ffmpeg command output: {result.stdout}")

def handler(event, context):
    try:
        logger.info("Function started")
        
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']
        
        logger.info(f"Processing video: {key}")
        
        # Input validation
        if not key.lower().endswith('.mp4'):
            raise ValueError(f"Invalid file format. Expected .mp4, got: {key}")
        
        video_filename = f"/tmp/{os.path.basename(key)}"
        s3_client.download_file(bucket, key, video_filename)
        logger.info(f"Downloaded {key} to {video_filename}")
        
        outfile = f"{os.path.splitext(os.path.basename(key))[0]}.jpg"
        output_file = f"/tmp/{outfile}"
        
        process_video(video_filename, output_file)
        
        logger.info(f"Frame extracted: {output_file}")
        
        stage_1_bucket = f"{bucket.split('-')[0]}-stage-1"
        logger.info(f"Stage-1 bucket: {stage_1_bucket}")
        
        s3_key = outfile
        s3_client.upload_file(output_file, stage_1_bucket, s3_key)
        logger.info(f"Uploaded {output_file} to {stage_1_bucket}/{s3_key}")
        
        # Invoke face-recognition function
        lambda_client.invoke(
            FunctionName='face-recognition',
            InvocationType='Event',
            Payload=json.dumps({
                'bucket_name': '1229568589-stage-1',
                'image_file_name': outfile
            })
        )
        logger.info(f"Invoked face-recognition function for {outfile}")
        
        # Clean up temporary files
        os.remove(video_filename)
        os.remove(output_file)
        
        return {
            'statusCode': 200,
            'body': 'Video processed, frame uploaded, and face-recognition triggered successfully'
        }

    except ValueError as ve:
        logger.error(f"Input validation error: {str(ve)}")
        return {
            'statusCode': 400,
            'body': f"Input validation error: {str(ve)}"
        }
    except Exception as e:
        logger.error(f"Error processing video: {str(e)}")
        return {
            'statusCode': 500,
            'body': f"Error processing video: {str(e)}"
        }