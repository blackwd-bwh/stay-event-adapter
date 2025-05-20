import os
import json
import boto3
import hashlib
import psycopg2
from datetime import datetime, timedelta
from aws_xray_sdk.core import patch_all

patch_all()

# AWS clients
sns_client = boto3.client('sns')
secrets_client = boto3.client('secretsmanager')
dynamodb = boto3.resource('dynamodb')

# Environment variables
SNS_TOPIC_ARN = os.environ['SNS_TOPIC_ARN']
DEDUP_TABLE_NAME = os.environ['DEDUP_TABLE_NAME']
REDSHIFT_SECRET_ARN = os.environ['REDSHIFT_SECRET_ARN']

# DynamoDB table reference
dedup_table = dynamodb.Table(DEDUP_TABLE_NAME)

# Redshift query
QUERY = """
SELECT *
FROM bwhrdw.fact_bookings
WHERE dim_dist_channel_3_key = '677'
  AND cancel_dt_key IS NULL
  AND rewards_id <> 'XXXXX'
  AND rewards_id IS NOT NULL
  AND departure_dt_key::DATE < CURRENT_DATE - 1
LIMIT 100;
"""

def handler(event, context):
    print("Received event:", json.dumps(event, indent=2))

    try:
        creds = fetch_redshift_credentials()
        rows = execute_query(creds, QUERY)

        for row in rows:
            print("Fetched row:", row)
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
        print("Unhandled error during Redshift processing:", e)
        raise

    return {"statusCode": 200, "body": "Processed Redshift rows successfully"}

def fetch_redshift_credentials():
    print(f"Fetching Redshift credentials from Secrets Manager: {REDSHIFT_SECRET_ARN}")
    response = secrets_client.get_secret_value(SecretId=REDSHIFT_SECRET_ARN)
    return json.loads(response['SecretString'])

def execute_query(creds, sql):
    print("Connecting to Redshift with the following parameters:")
    print(f"  host: {creds.get('host')}")
    print(f"  port: {creds.get('port')}")
    print(f"  dbname: {creds.get('dbname')}")
    print(f"  user: {creds.get('username')}")
    print(f"  password: [REDACTED]")

    try:
        conn = psycopg2.connect(
            host=creds['host'],
            port=creds['port'],
            dbname=creds['dbname'],
            user=creds['username'],
            password=creds['password'],
            connect_timeout=10
        )
        with conn.cursor() as cur:
            print("Executing Redshift query...")
            cur.execute(sql)
            columns = [desc[0] for desc in cur.description]
            rows = [dict(zip(columns, row)) for row in cur.fetchall()]
            print(f"Query returned {len(rows)} rows.")
        conn.close()
        print("Closed Redshift connection.")
        return rows
    except Exception as e:
        print("Error during Redshift query execution:", e)
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
        print("Error in stay completion filter:", e)
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
        message = json.dumps(payload)
        sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Message=message
        )
        print("Published event to SNS:", message)
    except Exception as e:
        print("Critical error publishing to SNS:", e)
        raise

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
        print("Error checking DynamoDB for duplicates:", e)
        return False

def mark_as_processed(event_hash):
    try:
        ttl = int((datetime.utcnow() + timedelta(days=3)).timestamp())
        dedup_table.put_item(Item={'EventHash': event_hash, 'TTL': ttl})
        print("Recorded hash in DynamoDB:", event_hash)
    except Exception as e:
        print("Error writing hash to DynamoDB:", e)
