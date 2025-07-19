"""Integration tests for the CDK stack."""

import os
import aws_cdk as core
from aws_cdk import assertions

from stay_event_adapter_poc.stay_event_adapter_poc_stack import StayEventAdapterPocStack

# example tests. To run these tests, uncomment this file along with the example
# resource in stay_event_adapter_poc/stay_event_adapter_poc_stack.py
def test_stack_synthesizes():
    """Verify that the stack can be synthesized."""

    app = core.App()
    layer_zip = os.path.join(
        os.path.dirname(__file__), "..", "..", "lambda-layers", "data-services-layer.zip"
    )
    # Create an empty layer artifact so the stack can synthesize
    with open(layer_zip, "a", encoding="utf-8"):
        pass
    stack = StayEventAdapterPocStack(
        app,
        "stay-event-adapter-poc",
        env=core.Environment(account="111111111111", region="us-west-2"),
    )
    template = assertions.Template.from_stack(stack)
    assert template.find_resources("AWS::SNS::Topic")

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
