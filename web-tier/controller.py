import time
import logging
import boto3
from botocore.exceptions import ClientError

# Set up logging configuration
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(threadName)s: %(message)s"
)

# AWS and autoscaling configuration
AWS_REGION = "us-east-1"
#REQ_QUEUE_NAME =
MAX_INSTANCES = 15
GRACE_PERIOD = 5

# Initialize AWS service clients for SQS and EC2 operations
logging.debug("Initializing AWS clients for SQS and EC2")
sqs_client = boto3.client("sqs", region_name=AWS_REGION)
ec2_client = boto3.client("ec2", region_name=AWS_REGION)
ec2_resource = boto3.resource("ec2", region_name=AWS_REGION)

# Constant: URL for the SQS request queue
#REQ_QUEUE_URL = 

# Variables for tracking timing in scaling decisions
last_active_time = time.time()
over_capacity_timer = None

def fetch_queue_length():
    """
    Retrieve the current approximate number of messages in the SQS request queue.
    
    Returns:
        int: The number of pending messages.
    """
    try:
        response = sqs_client.get_queue_attributes(
            QueueUrl=REQ_QUEUE_URL,
            AttributeNames=["ApproximateNumberOfMessages"]
        )
        queue_length = int(response["Attributes"].get("ApproximateNumberOfMessages", 0))
        logging.debug(f"Current queue length: {queue_length}")
        return queue_length
    except ClientError as err:
        logging.error(f"Failed to fetch queue attributes: {err}")
        return 0

def fetch_preprovisioned_instances():
    """
    Get the list of preprovisioned EC2 instances tagged for the application tier.
    
    Returns:
        list: Instances that are in 'stopped', 'pending', or 'running' states.
    """
    filters = [
        {"Name": "tag:Name", "Values": ["app-tier-instance-*"]},
        {"Name": "instance-state-name", "Values": ["stopped", "pending", "running"]}
    ]
    instance_list = list(ec2_resource.instances.filter(Filters=filters))
    logging.debug(f"Found {len(instance_list)} preprovisioned instances")
    return instance_list

def auto_scale_instances():
    """
    Continuously monitor the SQS queue and adjust the number of active EC2 instances accordingly.
    
    - If there are more pending messages than active instances, start additional instances.
    - If there are more active instances than needed for the current queue length,
      stop the surplus after a grace period.
    """
    global last_active_time, over_capacity_timer
    logging.info("Starting autoscaling process")
    while True:
        queue_length = fetch_queue_length()
        instance_list = fetch_preprovisioned_instances()
        active_instances = [inst for inst in instance_list if inst.state["Name"] in ["running", "pending"]]
        stopped_instances = [inst for inst in instance_list if inst.state["Name"] == "stopped"]

        logging.info(f"Pending messages: {queue_length}, Active instances: {len(active_instances)}")

        if queue_length > 0:
            last_active_time = time.time()

        desired_count = min(queue_length, MAX_INSTANCES)
        logging.debug(f"Desired active instance count: {desired_count}")

        if len(active_instances) < desired_count:
            instances_needed = desired_count - len(active_instances)
            logging.info(f"Insufficient active instances. Need to start {instances_needed} instance(s).")
            over_capacity_timer = None  # Reset the timer when scaling out
            if instances_needed > 0 and stopped_instances:
                instance_ids = [inst.id for inst in stopped_instances[:instances_needed]]
                logging.info(f"Starting instances: {instance_ids}")
                try:
                    ec2_client.start_instances(InstanceIds=instance_ids)
                    logging.debug("Successfully sent start command for instances")
                except ClientError as err:
                    logging.error(f"Error starting instances {instance_ids}: {err}")

        elif len(active_instances) > desired_count:
            if over_capacity_timer is None:
                over_capacity_timer = time.time()
                logging.debug("Detected excess active instances; initiating grace period timer.")
            elif time.time() - over_capacity_timer >= GRACE_PERIOD:
                instances_to_stop = active_instances[desired_count:]
                instance_ids = [inst.id for inst in instances_to_stop]
                if instance_ids:
                    logging.info(f"Stopping excess instances: {instance_ids}")
                    try:
                        ec2_client.stop_instances(InstanceIds=instance_ids)
                        logging.debug("Successfully sent stop command for instances")
                    except ClientError as err:
                        logging.error(f"Error stopping instances {instance_ids}: {err}")
        else:
            over_capacity_timer = None
            logging.debug("Active instances match desired count; no scaling action required.")

        time.sleep(2)

if __name__ == "__main__":
    auto_scale_instances()
