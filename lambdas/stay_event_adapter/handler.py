"""Lambda handler for processing stay-completed events from Redshift and publishing to SNS."""

import json
import os
import hashlib
from dataclasses import asdict
from datetime import datetime, timedelta
from decimal import Decimal

from query import STAY_COMPLETED_QUERY
from botocore.exceptions import BotoCoreError, ClientError

# Standard AWS clients
import boto3
import redshift_connector
from models.booking_row import BookingRow

dynamodb_client = boto3.client("dynamodb")
sns_client = boto3.client("sns")
secrets_client = boto3.client("secretsmanager")

# Environment vars
SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]
DEDUP_TABLE_NAME = os.environ["DEDUP_TABLE_NAME"]
REDSHIFT_SECRET_ARN = os.environ["REDSHIFT_SECRET_ARN"]

def get_redshift_connection():
    """Return a connection to Redshift using credentials from Secrets Manager."""

    response = secrets_client.get_secret_value(SecretId=REDSHIFT_SECRET_ARN)
    secret = json.loads(response["SecretString"])

    return redshift_connector.connect(
        host=secret["host"],
        database=secret["dbname"],
        user=secret["username"],
        password=secret["password"],
        port=int(secret["port"]),
    )

def handler(_event, _):
    """Entry point for the stay event adapter Lambda."""

    try:
        conn = get_redshift_connection()
        cursor = conn.cursor()

        query = STAY_COMPLETED_QUERY

        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        rows = [
            BookingRow.from_dict(dict(zip(columns, row)))
            for row in cursor.fetchall()
        ]
        print(f"Fetched {len(rows)} rows")

        for row in rows:
            print(json.dumps(asdict(row), default=json_safe))
            try:
                row_hash = hash_row(row)
                if not is_duplicate(row_hash):
                    event_payload = transform_to_event(row)
                    publish_event(event_payload)
                    mark_as_processed(row_hash)
                else:
                    print("Duplicate detected, skipping.")
            except redshift_connector.Error as error:
                print("Row processing error:", error)
                continue

        return {"statusCode": 200, "body": f"Processed {len(rows)} rows"}

    except redshift_connector.Error as error:
        print("Top-level Redshift error:", error)
        raise
    except (BotoCoreError, ClientError) as error:
        print("AWS client error:", error)
        raise

def transform_to_event(row: BookingRow):
    """Convert a BookingRow into the outbound event structure."""

    event = {
        "eventType": "StayCompleted"
    }
    row_data = {
        k: json_safe(v)
        for k, v in asdict(row).items()
    }
    event.update(row_data)
    return event


def publish_event(payload):
    """Publish the transformed event payload to SNS."""

    try:
        sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Message=json.dumps(payload, default=json_safe),
        )
        print("Published:", payload)
    except (BotoCoreError, ClientError) as error:
        print("Failed to publish:", error)

def hash_row(row: BookingRow):
    """Generate a SHA-256 hash for a booking row."""

    row_string = json.dumps(asdict(row), sort_keys=True, default=json_safe)
    return hashlib.sha256(row_string.encode("utf-8")).hexdigest()


def is_duplicate(event_hash):
    """Return True if the hash already exists in DynamoDB."""

    try:
        response = dynamodb_client.get_item(
            TableName=DEDUP_TABLE_NAME,
            Key={"EventHash": {"S": event_hash}},
        )
        return "Item" in response
    except ClientError as error:
        print("DynamoDB read error:", error)
        return False

def mark_as_processed(event_hash):
    """Record the processed hash in DynamoDB with a TTL."""

    try:
        ttl = int((datetime.utcnow() + timedelta(days=3)).timestamp())
        dynamodb_client.put_item(
            TableName=DEDUP_TABLE_NAME,
            Item={
                "EventHash": {"S": event_hash},
                "TTL": {"N": str(ttl)},
            },
        )
    except ClientError as error:
        print("DynamoDB write error:", error)

def json_safe(obj):
    """Convert Decimal objects to float for JSON serialization."""

    if isinstance(obj, Decimal):
        return float(obj)
    return str(obj)
