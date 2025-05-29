"""Deduplication logic for checking and storing event hashes in DynamoDB."""

from datetime import datetime, timedelta
import hashlib
import os
from botocore.exceptions import ClientError
import boto3
from aws_xray_sdk.core import patch_all

patch_all()

dynamodb = boto3.resource("dynamodb")
dedup_table_name = os.environ["DEDUP_TABLE_NAME"]
dedup_table = dynamodb.Table(dedup_table_name)

def get_row_hash(row: str) -> str:
    """Generate a SHA-256 hash from the entire row string."""
    return hashlib.sha256(row.encode("utf-8")).hexdigest()

def is_duplicate(row: str) -> bool:
    """
    Check if the row hash exists in DynamoDB. 
    If not, insert it and return False. If yes, return True.
    """
    row_hash = get_row_hash(row)
    try:
        response = dedup_table.get_item(Key={"EventHash": row_hash})
        if "Item" in response:
            return True

        # Insert with TTL (optional): expires in 7 days
        ttl = int((datetime.utcnow() + timedelta(days=7)).timestamp())
        dedup_table.put_item(
            Item={"EventHash": row_hash, "TTL": ttl}
        )
        return False
    except ClientError as e:
        print(f"Error checking/inserting dedup entry: {e}")
        return False
