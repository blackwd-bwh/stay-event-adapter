"""Simple SNS subscriber used for testing."""

import json


def handler(event, _context):
    """Log the SNS message payload."""

    print("SNS event received:")
    print(json.dumps(event, indent=2))

    for record in event.get("Records", []):
        message = record.get("Sns", {}).get("Message")
        print("Message payload:", message)
