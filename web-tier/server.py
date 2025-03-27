import os
import time
import threading
import logging
from flask import Flask, request
import boto3
from botocore.exceptions import ClientError

# Configure logging settings
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(threadName)s: %(message)s"
)

# AWS and application configuration
AWS_REGION = "us-east-1"
#S3_BUCKET_NAME =
#REQ_QUEUE_NAME =
#RESP_QUEUE_NAME =
POLL_INTERVAL_SEC = 0.2
MAX_RESULT_WAIT_SEC = 60

# Initialize AWS S3 and SQS clients
logging.debug("Setting up AWS S3 and SQS clients")
s3_client = boto3.client("s3", region_name=AWS_REGION)
sqs_client = boto3.client("sqs", region_name=AWS_REGION)

# Constant URLs for the SQS queues (must be in all caps)
#REQ_QUEUE_URL = 
#RESP_QUEUE_URL = 

# Flask application instance
app = Flask(__name__)
# Dictionary to hold recognition results with file base names as keys
prediction_results = {}

def background_polling():
    """
    Continuously polls the SQS response queue for face recognition results,
    stores them in the global prediction_results dictionary, and deletes processed messages.
    """
    logging.info("Starting background polling thread for SQS responses")
    while True:
        try:
            logging.debug("Polling response queue for new messages")
            response = sqs_client.receive_message(
                QueueUrl=RESP_QUEUE_URL,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=5
            )
            messages = response.get("Messages", [])
            if messages:
                logging.debug(f"Retrieved {len(messages)} message(s) from response queue")
            for msg in messages:
                body = msg["Body"]
                logging.debug(f"Message body received: {body}")
                if ":" in body:
                    base_filename, prediction = body.split(":", 1)
                    prediction_results[base_filename] = prediction
                    logging.info(f"Result stored - {base_filename}: {prediction}")
                try:
                    sqs_client.delete_message(
                        QueueUrl=RESP_QUEUE_URL,
                        ReceiptHandle=msg["ReceiptHandle"]
                    )
                    logging.debug("Deleted processed message from response queue")
                except ClientError as err:
                    logging.error(f"Error deleting message: {err}")
        except Exception as ex:
            logging.error(f"Exception during SQS polling: {ex}")
        time.sleep(POLL_INTERVAL_SEC)

# Start the background polling thread
polling_thread = threading.Thread(target=background_polling, daemon=True, name="SQS-Poller")
polling_thread.start()

@app.route("/", methods=["POST"])
def handle_prediction_request():
    """
    HTTP endpoint to handle face recognition requests.
    - Uploads the image to S3.
    - Sends a message to the request SQS queue.
    - Waits for the corresponding prediction result from the background polling thread.
    """
    logging.info("Received POST request for face recognition")
    if "inputFile" not in request.files:
        logging.warning("Request missing 'inputFile' parameter")
        return "Missing inputFile", 400

    uploaded_file = request.files["inputFile"]
    uploaded_filename = uploaded_file.filename
    if not uploaded_filename:
        logging.warning("Uploaded file has no filename")
        return "Empty filename", 400

    logging.info(f"Processing uploaded file: {uploaded_filename}")
    try:
        logging.debug("Uploading image to S3 bucket")
        s3_client.upload_fileobj(uploaded_file, S3_BUCKET_NAME, uploaded_filename)
        logging.info("Image uploaded successfully to S3")
    except ClientError as err:
        logging.error(f"S3 upload failed: {err}")
        return "Failed to upload file", 500

    base_filename = os.path.splitext(uploaded_filename)[0]
    try:
        logging.debug("Sending face recognition request to SQS")
        sqs_client.send_message(QueueUrl=REQ_QUEUE_URL, MessageBody=uploaded_filename)
        logging.info("SQS request message sent successfully")
    except ClientError as err:
        logging.error(f"Failed to send SQS message: {err}")
        return "Failed to send SQS message", 500

    start_time = time.time()
    logging.debug("Waiting for recognition result")
    while time.time() - start_time < MAX_RESULT_WAIT_SEC:
        if base_filename in prediction_results:
            prediction = prediction_results.pop(base_filename)
            logging.info(f"Returning result for {base_filename}: {prediction}")
            return f"{base_filename}:{prediction}", 200
        time.sleep(0.5)

    logging.error("Timeout waiting for face recognition result")
    return "Timeout waiting for recognition result", 504

if __name__ == "__main__":
    logging.info("Starting Flask server on port 8000")
    app.run(host="0.0.0.0", port=8000, threaded=True)
