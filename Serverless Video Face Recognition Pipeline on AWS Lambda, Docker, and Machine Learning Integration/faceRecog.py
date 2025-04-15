import os
import numpy as np
import cv2
from PIL import Image
from facenet_pytorch import MTCNN, InceptionResnetV1
import torch
import boto3


# Initialize the S3 client
s3 = boto3.client('s3')

stage_1_bucket = '1229568589-stage-1'
output_bucket = '1229568589-output'

os.environ['TORCH_HOME'] = '/tmp'

mtcnn = MTCNN(image_size=240, margin=0, min_face_size=20)
resnet = InceptionResnetV1(pretrained='vggface2').eval()

def face_recognition_function(key_path):
    img = cv2.imread(key_path, cv2.IMREAD_COLOR)
    boxes, _ = mtcnn.detect(img)

    key = os.path.splitext(os.path.basename(key_path))[0].split(".")[0]
    img = Image.fromarray(cv2.cvtColor(img, cv2.COLOR_BGR2RGB))
    face, prob = mtcnn(img, return_prob=True, save_path=None)
    saved_data = torch.load('./data.pt')
    if face is not None:
        emb = resnet(face.unsqueeze(0)).detach()
        embedding_list = saved_data[0]
        name_list = saved_data[1]
        dist_list = []
        for idx, emb_db in enumerate(embedding_list):
            dist = torch.dist(emb, emb_db).item()
            dist_list.append(dist)
        idx_min = dist_list.index(min(dist_list))

        with open("/tmp/" + key + ".txt", 'w+') as f:
            f.write(name_list[idx_min])
        return name_list[idx_min]
    else:
        print(f"No face is detected")
    return

def lambda_handler(event, context):
    bucket_name = event['bucket_name']
    image_file_name = event['image_file_name']
    download_path = f"/tmp/{image_file_name}"
    
    s3.download_file(Bucket=bucket_name, Key=image_file_name, Filename=download_path)
    print(f"Downloaded {image_file_name} from bucket {bucket_name} to {download_path}")
    
    recognition_result = face_recognition_function(download_path)

    if recognition_result:
        output_file_key = os.path.splitext(image_file_name)[0] + ".txt"
        upload_path = '/tmp/' + output_file_key
        s3.upload_file(upload_path, output_bucket, output_file_key)