import os
import time
import logging
from io import BytesIO

import boto3
import torch
from botocore.exceptions import ClientError
from facenet_pytorch import MTCNN, InceptionResnetV1
from PIL import Image

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(threadName)s: %(message)s"
)

# AWS configuration and resource naming
#AWS_REGION = 
#IN_BUCKET_NAME = 
#OUT_BUCKET_NAME = 
#REQ_QUEUE_NAME = 
#RESP_QUEUE_NAME = 

# Initialize AWS clients
logging.debug("Initializing AWS S3 and SQS clients")
s3_client = boto3.client("s3", region_name=AWS_REGION)
sqs_client = boto3.client("sqs", region_name=AWS_REGION)

# SQS queue URLs in all caps as per requirements
#REQ_QUEUE_URL = 
#RESP_QUEUE_URL = 

# Initialize face detection and recognition models
logging.debug("Initializing MTCNN and InceptionResnetV1 models")
mtcnn_detector = MTCNN(image_size=240, margin=0, min_face_size=20)
face_recognizer = InceptionResnetV1(pretrained='vggface2').eval()

# Load saved face embeddings and names
saved_data = torch.load('data.pt')
db_embeddings = saved_data[0]
db_names = saved_data[1]

def match_face(image_bytes):
    """
    Given raw image bytes, detect a face and perform face recognition.
    
    Args:
        image_bytes (bytes): Raw image data.
        
    Returns:
        tuple: Recognized name and the corresponding distance measure.
    """
    image = Image.open(BytesIO(image_bytes))
    with torch.no_grad():
        face_tensor, prob = mtcnn_detector(image, return_prob=True)
        embedding = face_recognizer(face_tensor.unsqueeze(0))
    distances = [torch.dist(embedding, emb_db).item() for emb_db in db_embeddings]
    min_index = distances.index(min(distances))
    recognized_name = db_names[min_index]
    min_distance = min(distances)
    logging.debug(f"Face matched: {recognized_name} with distance {min_distance}")
    return recognized_name, min_distance

def handle_face_recognition_requests():
    """
    Polls the SQS request queue, processes each face recognition request by:
      - Retrieving the image from the S3 input bucket.
      - Performing face recognition.
      - Storing the result in the S3 output bucket.
      - Sending the recognition result back via the SQS response queue.
    """
    while True:
        try:
            response = sqs_client.receive_message(
                QueueUrl=REQ_QUEUE_URL,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=5
            )
        except ClientError as err:
            logging.error(f"Error receiving message from SQS: {err}")
            time.sleep(1)
            continue

        messages = response.get("Messages", [])
        if not messages:
            continue

        for msg in messages:
            file_name = msg["Body"].strip()
            base_file_name = os.path.splitext(file_name)[0]
            logging.info(f"Processing file: {file_name}")

            try:
                s3_response = s3_client.get_object(Bucket=IN_BUCKET_NAME, Key=file_name)
                image_data = s3_response["Body"].read()
                logging.debug("Image data retrieved from S3")
            except ClientError as err:
                logging.error(f"Failed to retrieve file from S3: {err}")
                sqs_client.delete_message(QueueUrl=REQ_QUEUE_URL, ReceiptHandle=msg["ReceiptHandle"])
                continue

            recognized_name, distance = match_face(image_data)

            try:
                s3_client.put_object(Bucket=OUT_BUCKET_NAME, Key=base_file_name, Body=recognized_name)
                logging.info(f"Stored recognition result for {base_file_name} in S3")
            except ClientError as err:
                logging.error(f"Failed to store result in S3: {err}")

            message_body = f"{base_file_name}:{recognized_name}"
            try:
                sqs_client.send_message(QueueUrl=RESP_QUEUE_URL, MessageBody=message_body)
                logging.info(f"Sent recognition result to SQS: {message_body}")
            except ClientError as err:
                logging.error(f"Failed to send message to SQS: {err}")

            try:
                sqs_client.delete_message(QueueUrl=REQ_QUEUE_URL, ReceiptHandle=msg["ReceiptHandle"])
                logging.debug("Deleted processed request from SQS")
            except ClientError as err:
                logging.error(f"Failed to delete SQS message: {err}")
        time.sleep(0.5)

if __name__ == "__main__":
    handle_face_recognition_requests()
