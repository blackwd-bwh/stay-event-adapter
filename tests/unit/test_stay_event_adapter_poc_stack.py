import aws_cdk as core
import aws_cdk.assertions as assertions

from stay_event_adapter_poc.stay_event_adapter_poc_stack import StayEventAdapterPocStack

# example tests. To run these tests, uncomment this file along with the example
# resource in stay_event_adapter_poc/stay_event_adapter_poc_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = StayEventAdapterPocStack(app, "stay-event-adapter-poc")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
