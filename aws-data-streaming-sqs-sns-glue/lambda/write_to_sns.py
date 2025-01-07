import json
import os
import random
import uuid
import boto3
from datetime import datetime

# Initialize the SNS client
sns_client = boto3.client('sns')

# Get the SNS topic ARN from the environment variable
SNS_TOPIC_ARN = 'arn:aws:sns:us-east-2:988410076871:test-sns-topic.fifo'

def generate_random_data():
    """
    Generates random data with additional fields to be sent to the SNS topic.
    """
    return {
        "id": str(uuid.uuid4()),  # Unique identifier
        "timestamp": datetime.utcnow().isoformat(),  # ISO 8601 timestamp
        "value": round(random.uniform(1.0, 100.0), 2),  # Random floating-point value
        "status": random.choice(["active", "inactive", "error"]),  # Random status
        "user": f"user_{random.randint(1000, 9999)}",  # Simulated user identifier
        "priority": random.choice(["low", "medium", "high"]),  # Random priority level
        "source": random.choice(["sensor_1", "sensor_2", "sensor_3"]),  # Data source
        "metadata": {
            "latitude": round(random.uniform(-90.0, 90.0), 6),  # Random latitude
            "longitude": round(random.uniform(-180.0, 180.0), 6),  # Random longitude
            "temperature": round(random.uniform(-30.0, 50.0), 2),  # Random temperature
            "humidity": round(random.uniform(10.0, 90.0), 2)  # Random humidity percentage
        }
    }

def lambda_handler(event, context):
    """
    Lambda handler to send random data to an SNS topic.
    """
    try:
        # Generate random data
        random_data = generate_random_data()
        print(f"Generated random data: {random_data}")
        
        # Publish the random data to the SNS topic
        response = sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Message=json.dumps(random_data),
            Subject="Enhanced Random Data Notification",
            MessageGroupId="group-1",  # Replace with a dynamic value if necessary
            MessageDeduplicationId=str(uuid.uuid4())  # Ensure uniqueness for FIFO deduplication
        )
        
        print(f"Message sent to SNS: {response['MessageId']}")
        
        return {
            "status": "Success",
            "messageId": response['MessageId'],
            "data": random_data
        }
    
    except Exception as e:
        print(f"Error sending message to SNS: {e}")
        return {
            "status": "Error",
            "error": str(e)
        }
