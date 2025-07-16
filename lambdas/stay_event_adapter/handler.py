"""Lambda handler for processing stay-completed events from Redshift and publishing to SNS."""

import json
import os
import hashlib
from dataclasses import asdict
from datetime import datetime, timedelta

# Monkey patch ssl to use certifi for all clients
import ssl
import certifi
ssl._create_default_https_context = lambda: ssl.create_default_context(cafile=certifi.where())

import boto3
import redshift_connector
from models.booking_row import BookingRow

print("Verifying cert file path:")
print("Exists:", os.path.exists(certifi.where()))
print("Dir listing (certifi):", os.listdir(os.path.dirname(certifi.where())))

# AWS clients (no need to manually pass `verify=...` now)
boto3_session = boto3.session.Session()
dynamodb = boto3_session.resource("dynamodb")
sns_client = boto3_session.client("sns")
secrets_client = boto3_session.client("secretsmanager")

# Environment vars
SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]
DEDUP_TABLE_NAME = os.environ["DEDUP_TABLE_NAME"]
REDSHIFT_SECRET_ARN = os.environ["REDSHIFT_SECRET_ARN"]

dedup_table = dynamodb.Table(DEDUP_TABLE_NAME)

def get_redshift_connection():
    response = secrets_client.get_secret_value(SecretId=REDSHIFT_SECRET_ARN)
    secret = json.loads(response["SecretString"])

    return redshift_connector.connect(
        host=secret["host"],
        database=secret["dbname"],
        user=secret["username"],
        password=secret["password"],
        port=int(secret["port"])
    )

def handler(event, _):
    print("Received event:", json.dumps(event, indent=2))

    try:
        conn = get_redshift_connection()
        cursor = conn.cursor()
        query = """
        SELECT
            rewards_id,
            resv_detail_id,
            property_id,
            arrival_dt_key,
            departure_dt_key,
            rate_code,
            dim_dist_channel_3_key
        FROM bwhrdw.fact_bookings
        WHERE departure_dt_key::DATE = CURRENT_DATE
            AND rewards_id IS NOT NULL
            AND rewards_id <> 'XXXXX'
            AND cancel_dt_key IS NULL
            AND rate_code <> 'FX'
            AND dim_dist_channel_1_key <> '4'
        """
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        rows = [BookingRow(**dict(zip(columns, row))) for row in cursor.fetchall()]
        print(f"Fetched {len(rows)} rows")

        for row in rows:
            print(json.dumps(asdict(row), default=str))
            try:
                row_hash = hash_row(row)
                if not is_duplicate(row_hash):
                    event_payload = transform_to_event(row)
                    publish_event(event_payload)
                    mark_as_processed(row_hash)
                else:
                    print("Duplicate detected, skipping.")
            except redshift_connector.Error as e:
                print("Row processing error:", e)
                continue

        return {"statusCode": 200, "body": f"Processed {len(rows)} rows"}

    except Exception as e:
        print("Top-level error:", e)
        raise

def transform_to_event(row: BookingRow):
    return {
        "eventType": "StayCompleted",
        "rewardsId": row.rewards_id,
        "reservationDetailId": row.resv_detail_id,
        "propertyId": row.property_id,
        "arrivalDate": row.arrival_dt_key,
        "departureDate": row.departure_dt_key,
        "rateCode": row.rate_code,
        "distributionChannel": row.dim_dist_channel_3_key
    }

def publish_event(payload):
    try:
        sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Message=json.dumps(payload)
        )
        print("Published:", payload)
    except Exception as e:
        print("Failed to publish:", e)

def hash_row(row: BookingRow):
    row_string = json.dumps(asdict(row), sort_keys=True, default=str)
    print(hashlib.sha256(row_string.encode('utf-8')).hexdigest());
    return hashlib.sha256(row_string.encode('utf-8')).hexdigest()

def is_duplicate(event_hash):
    try:
        response = dedup_table.get_item(Key={'EventHash': event_hash})
        return 'Item' in response
    except Exception as e:
        print("DynamoDB read error:", e)
        return False

def mark_as_processed(event_hash):
    try:
        ttl = int((datetime.utcnow() + timedelta(days=3)).timestamp())
        dedup_table.put_item(Item={'EventHash': event_hash, 'TTL': ttl})
    except Exception as e:
        print("DynamoDB write error:", e)
