import json

# AWS X-Ray instrumentation
# from aws_xray_sdk.core import patch_all
# patch_all()

def handler(event, context):
    print("SNS event received:")
    print(json.dumps(event, indent=2))

    for record in event.get("Records", []):
        message = record.get("Sns", {}).get("Message")
        print("Message payload:", message)