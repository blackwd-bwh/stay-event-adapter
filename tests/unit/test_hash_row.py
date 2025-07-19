import os
import sys
import json
import hashlib
from dataclasses import asdict

# Add lambda module path for imports
THIS_DIR = os.path.dirname(__file__)
LAMBDA_PATH = os.path.join(THIS_DIR, '..', '..', 'lambdas', 'stay_event_adapter')
sys.path.append(os.path.abspath(LAMBDA_PATH))

# boto3 clients in handler require a region
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("SNS_TOPIC_ARN", "arn:aws:sns:us-east-1:123456789012:test")
os.environ.setdefault("DEDUP_TABLE_NAME", "table")
os.environ.setdefault("REDSHIFT_SECRET_ARN", "secret")

import handler
from models.booking_row import BookingRow


def test_hash_row_returns_expected_digest():
    row = BookingRow(resv_nbr="1")
    digest = handler.hash_row(row)

    row_string = json.dumps(asdict(row), sort_keys=True, default=handler.json_safe)
    expected = hashlib.sha256(row_string.encode("utf-8")).hexdigest()
    assert digest == expected
