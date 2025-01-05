import boto3
import json
import os
from datetime import datetime

# Initialize clients for SQS and S3
sqs_client = boto3.client('sqs')
s3_client = boto3.client('s3')

# Environment variables
SQS_QUEUE_URL = 'https://sqs.us-east-2.amazonaws.com/988410076871/test_q'
S3_BUCKET_NAME = 'learning-buk'

def lambda_handler(event, context):
    """
    Lambda function to read all messages from SQS and write them to S3.
    """
    try:
        total_messages_processed = 0  # Track total messages processed

        while True:
            # Receive messages from SQS
            response = sqs_client.receive_message(
                QueueUrl=SQS_QUEUE_URL,
                MaxNumberOfMessages=10,  # Maximum messages per batch
                WaitTimeSeconds=3        # Enable long polling
            )
            
            messages = response.get('Messages', [])
            if not messages:
                print("No more messages to process.")
                break
            
            for message in messages:
                message_body = json.loads(json.loads(message['Body'])['Message'])
                receipt_handle = message['ReceiptHandle']
                
                # Generate unique S3 key for each message
                timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H-%M-%S')
                s3_key = f"sqs_landing/test_q/{timestamp}-{message['MessageId']}.json"
                
                # Write the message body to S3
                s3_client.put_object(
                    Bucket=S3_BUCKET_NAME,
                    Key=s3_key,
                    Body=json.dumps(message_body),
                    ContentType='application/json'
                )
                print(f"Message written to S3: {s3_key}")
                
                # Delete the message from SQS after successful processing
                dlt_response = sqs_client.delete_message(
                    QueueUrl=SQS_QUEUE_URL,
                    ReceiptHandle=receipt_handle
                )
                print(f"Message deleted from SQS: {message['MessageId']}, status code: {dlt_response['ResponseMetadata']['HTTPStatusCode']}")
                
                total_messages_processed += 1
        
        return {
            "status": "Messages processed",
            "total_messages": total_messages_processed
        }
    
    except Exception as e:
        print(f"Error processing messages: {e}")
        return {
            "status": "Warning",
            "error": str(e)
        }
