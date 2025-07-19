import aws_cdk as core
import aws_cdk.assertions as assertions
import os

from stay_event_adapter_poc.stay_event_adapter_poc_stack import StayEventAdapterPocStack

# example tests. To run these tests, uncomment this file along with the example
# resource in stay_event_adapter_poc/stay_event_adapter_poc_stack.py
def test_stack_synthesizes():
    app = core.App()
    layer_zip = os.path.join(
        os.path.dirname(__file__), "..", "..", "lambda-layers", "data-services-layer.zip"
    )
    # Create an empty layer artifact so the stack can synthesize
    open(layer_zip, "a").close()
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
