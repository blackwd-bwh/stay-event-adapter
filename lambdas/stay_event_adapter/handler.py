import os
import csv
import json
import boto3
import hashlib
from io import StringIO
from datetime import datetime, timedelta

from aws_xray_sdk.core import patch_all
patch_all()

s3_client = boto3.client('s3')
sns_client = boto3.client('sns')
dynamodb = boto3.resource('dynamodb')

BUCKET_NAME = os.environ['BUCKET_NAME']
SNS_TOPIC_ARN = os.environ['SNS_TOPIC_ARN']
DEDUP_TABLE_NAME = os.environ['DEDUP_TABLE_NAME']

dedup_table = dynamodb.Table(DEDUP_TABLE_NAME)

def handler(event, context):
    print("Received event:", json.dumps(event, indent=2))

    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]

        print(f"Processing file from S3 - Bucket: {bucket}, Key: {key}")

        if not key.endswith(".csv"):
            print(f"Skipping non-CSV file: {key}")
            continue

        try:
            csv_obj = s3_client.get_object(Bucket=bucket, Key=key)
            body = csv_obj['Body'].read().decode('utf-8')
            print("CSV Body (first 500 chars):", body[:500])
            csv_reader = csv.DictReader(StringIO(body))
        except Exception as e:
            print(f"Critical Error: Unable to read or parse CSV file from {bucket}/{key}:", e)
            raise Exception(f"Critical: Failed to parse CSV for object {bucket}/{key}") from e

        for row in csv_reader:
            print("Parsed row:", row)

            try:
                if is_stay_completed(row):
                    row_hash = hash_row(row)
                    if not is_duplicate(row_hash):
                        event_payload = transform_to_event(row)
                        publish_event(event_payload)
                        mark_as_processed(row_hash)
                    else:
                        print("Duplicate detected, skipping event.")
                else:
                    print("Row did not qualify as StayCompleted, skipping.")
            except Exception as e:
                print(f"Error processing row: {e}")
                continue

    return {"statusCode": 200, "body": "Processed successfully"}

def is_stay_completed(row):
    try:
        return (
            row.get('departure_dt_key') and
            row['departure_dt_key'] < current_date_key() and
            not row.get('cancel_dt_key') and
            row.get('rate_code') != 'FX' and
            row.get('dim_dist_channel_1_key') != '4' and
            row.get('rewards_id') and
            row['rewards_id'] != 'XXXXX'
        )
    except Exception as e:
        print("Error in filtering row:", e)
        return False

def transform_to_event(row):
    return {
        "eventType": "StayCompleted",
        "rewardsId": row.get('rewards_id'),
        "reservationId": row.get("reservation_id"),
        "propertyId": row.get("property_id"),
        "arrivalDate": row.get("arrival_dt_key"),
        "departureDate": row.get("departure_dt_key"),
        "rateCode": row.get("rate_code"),
        "distributionChannel": row.get("dim_dist_channel_3_key")
    }

def publish_event(payload):
    try:
        sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Message=json.dumps(payload)
        )
        print("Published event:", payload)
    except Exception as e:
        print("Critical Error publishing to SNS:", e)
        raise Exception("Critical: Failed to publish event to SNS") from e

def current_date_key():
    return datetime.utcnow().strftime('%Y%m%d')

def hash_row(row):
    row_string = json.dumps(row, sort_keys=True)
    return hashlib.sha256(row_string.encode('utf-8')).hexdigest()

def is_duplicate(event_hash):
    try:
        response = dedup_table.get_item(Key={'EventHash': event_hash})
        return 'Item' in response
    except Exception as e:
        print("Error checking DynamoDB:", e)
        return False

def mark_as_processed(event_hash):
    try:
        ttl = int((datetime.utcnow() + timedelta(days=3)).timestamp())
        dedup_table.put_item(Item={'EventHash': event_hash, 'TTL': ttl})
        print("Recorded hash in DynamoDB:", event_hash)
    except Exception as e:
        print("Error writing to DynamoDB:", e)
