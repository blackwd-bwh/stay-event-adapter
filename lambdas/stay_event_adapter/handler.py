"""Lambda handler for processing stay-completed events from Redshift and publishing to SNS."""

import json
import os
import hashlib
from dataclasses import asdict
from datetime import datetime, timedelta
from decimal import Decimal

# Monkey patch ssl to use certifi for all clients
import ssl
import certifi
ssl._create_default_https_context = lambda: ssl.create_default_context(cafile=certifi.where())
os.environ['AWS_CA_BUNDLE'] = certifi.where()

# Patch botocore session to use certifi CA bundle globally
import botocore.session
botocore_session = botocore.session.get_session()
botocore_session.set_config_variable("ca_bundle", certifi.where())

# boto3 with patched session
import boto3
import redshift_connector
from models.booking_row import BookingRow

boto3_session = boto3.Session(botocore_session=botocore_session)
dynamodb_client = boto3_session.client("dynamodb")
sns_client = boto3_session.client("sns")
secrets_client = boto3_session.client("secretsmanager")

# Environment vars
SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]
DEDUP_TABLE_NAME = os.environ["DEDUP_TABLE_NAME"]
REDSHIFT_SECRET_ARN = os.environ["REDSHIFT_SECRET_ARN"]

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
    try:
        conn = get_redshift_connection()
        cursor = conn.cursor()

        query = """
        SELECT
            resv_nbr,
            resv_detail_id,
            booking_dt_key,
            arrival_dt_key,
            departure_dt_key,
            cancel_dt_key,
            conf_nbr,
            nbr_adults,
            nbr_children,
            dim_property_key,
            property_id,
            dist_channel_0,
            rewards_id,
            dim_ta_key,
            ta_id,
            dim_ca_key,
            ca_id,
            dim_rate_code_key,
            dim_rewards_key,
            rate_code,
            dim_room_type_key,
            room_category,
            dim_country_origin_key,
            country_origin_code,
            record_update_dttm,
            stay,
            stay_before_cancellation,
            length_of_stay,
            length_of_stay_before_cancellation,
            rev_local_curr,
            rev_usd,
            rev_before_cancellation_local_curr,
            rev_before_cancellation_usd,
            roomnights,
            roomnights_before_cancellation,
            adr_usd,
            adr_local_curr,
            adr_before_cancellation_usd,
            adr_before_cancellation_local_curr,
            vat_usd,
            vat_local_curr,
            vat_usd_departure,
            vat_local_curr_departure,
            dim_dist_channel_1_key,
            dim_dist_channel_2_key,
            dim_dist_channel_3_key,
            dim_dist_channel_4_key,
            dim_business_source_key,
            business_source_code,
            booking_exchange_rate,
            booking_currency_code,
            property_currency_exchange_rate,
            property_currency_code,
            name_id,
            ota_enroll_ind,
            wh_conf_nbr,
            sx_ind,
            lynx_ad_code,
            operator_user_id,
            guarantee_code,
            rate_code_original,
            dim_rate_code_original_key,
            dim_parent_acct_key,
            dim_sales_mgr_key,
            comm_ind,
            dim_operator_worker_key,
            operator_worker_id,
            dim_rate_header_market_segment_key,
            dim_update_operator_worker_key,
            update_operator_worker_id,
            gds_record_locator,
            source_record_update_dttm,
            dim_guest_key,
            guest_id,
            cancel_by_date,
            leg_no,
            original_leg_no,
            booking_dttm,
            dim_guest_departure_dt_key,
            batch_ind,
            batch_update_dttm,
            rev_usd_fx,
            rev_local_curr_fx,
            day_use_ind,
            no_show_ind,
            external_reference,
            crx_resv_ind
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
            except redshift_connector.Error as e:
                print("Row processing error:", e)
                continue

        return {"statusCode": 200, "body": f"Processed {len(rows)} rows"}

    except Exception as e:
        print("Top-level error:", e)
        raise

def transform_to_event(row: BookingRow):
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
    try:
        sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Message=json.dumps(payload, default=json_safe)
        )
        print("Published:", payload)
    except Exception as e:
        print("Failed to publish:", e)

def hash_row(row: BookingRow):
    row_string = json.dumps(asdict(row), sort_keys=True, default=json_safe)
    print(hashlib.sha256(row_string.encode('utf-8')).hexdigest())
    return hashlib.sha256(row_string.encode('utf-8')).hexdigest()

def is_duplicate(event_hash):
    try:
        response = dynamodb_client.get_item(
            TableName=DEDUP_TABLE_NAME,
            Key={"EventHash": {"S": event_hash}}
        )
        return "Item" in response
    except Exception as e:
        print("DynamoDB read error:", e)
        return False

def mark_as_processed(event_hash):
    try:
        ttl = int((datetime.utcnow() + timedelta(days=3)).timestamp())
        dynamodb_client.put_item(
            TableName=DEDUP_TABLE_NAME,
            Item={
                "EventHash": {"S": event_hash},
                "TTL": {"N": str(ttl)}
            }
        )
    except Exception as e:
        print("DynamoDB write error:", e)

def json_safe(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    return str(obj)
