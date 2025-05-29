"""Lambda handler for processing stay-completed events from Redshift and publishing to SNS."""

import json
import os
import hashlib
from datetime import datetime, timedelta

import boto3
import redshift_connector

dynamodb = boto3.resource("dynamodb")
sns_client = boto3.client("sns")
secrets_client = boto3.client("secretsmanager")

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

def handler(event, context):
    print("Received event:", json.dumps(event, indent=2))

    try:
        conn = get_redshift_connection()
        cursor = conn.cursor()
        query = """
        SELECT * 
        FROM bwhrdw.fact_bookings
        WHERE dim_dist_channel_3_key = '677'
            AND cancel_dt_key IS NULL
            AND rewards_id <> 'XXXXX'
            AND rewards_id IS NOT NULL
            AND departure_dt_key::DATE < CURRENT_DATE - 1
        LIMIT 100;
        """
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
        print(f"Fetched {len(rows)} rows")

        for row in rows:
            try:
                if is_stay_completed(row):
                    row_hash = hash_row(row)
                    if not is_duplicate(row_hash):
                        event_payload = transform_to_event(row)
                        publish_event(event_payload)
                        mark_as_processed(row_hash)
                    else:
                        print("Duplicate detected, skipping.")
                else:
                    print("Filtered out row.")
            except redshift_connector.Error as e:
                print("Row processing error:", e)
                continue

        return {"statusCode": 200, "body": f"Processed {len(rows)} rows"}

    except Exception as e:
        print("Top-level error:", e)
        raise

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
        print("Published:", payload)
    except Exception as e:
        print("Failed to publish:", e)

def hash_row(row):
    row_string = json.dumps(row, sort_keys=True)
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

def current_date_key():
    return datetime.utcnow().strftime('%Y%m%d')
