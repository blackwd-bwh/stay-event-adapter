import aws_cdk as core
import aws_cdk.assertions as assertions
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_lambda as lambda_fn
from unittest.mock import patch

from stay_event_adapter_poc.stay_event_adapter_poc_stack import StayEventAdapterPocStack

# example tests. To run these tests, uncomment this file along with the example
# resource in stay_event_adapter_poc/stay_event_adapter_poc_stack.py
def test_sqs_queue_created():
    app = core.App()
    def create_vpc(scope, id, **kwargs):
        return ec2.Vpc(scope, id, max_azs=1)

    def create_subnet(scope, id, subnet_id):
        return ec2.Subnet(scope, id,
                          availability_zone="us-east-1a",
                          cidr_block="10.0.0.0/24",
                          vpc_id="vpc-123")

    def create_sg(scope, id, security_group_id, **kwargs):
        vpc = create_vpc(scope, "SGVpc")
        return ec2.SecurityGroup(scope, id, vpc=vpc)

    dummy_stack = core.Stack(app, "DummyStack")
    dummy_layer = lambda_fn.LayerVersion.from_layer_version_arn(
        dummy_stack, "Layer", "arn:aws:lambda:us-east-1:123:layer:dummy:1"
    )

    with patch("stay_event_adapter_poc.stay_event_adapter_poc_stack.ec2.Vpc.from_lookup", side_effect=create_vpc), \
         patch("stay_event_adapter_poc.stay_event_adapter_poc_stack.ec2.Subnet.from_subnet_id", side_effect=create_subnet), \
         patch("stay_event_adapter_poc.stay_event_adapter_poc_stack.ec2.SecurityGroup.from_security_group_id", side_effect=create_sg), \
         patch("stay_event_adapter_poc.stay_event_adapter_poc_stack.lambda_fn.Code.from_asset", return_value=lambda_fn.Code.from_inline("dummy")), \
         patch("stay_event_adapter_poc.stay_event_adapter_poc_stack.lambda_fn.LayerVersion", return_value=dummy_layer):

        stack = StayEventAdapterPocStack(
            app,
            "stay-event-adapter-poc",
            env=core.Environment(account="123456789012", region="us-east-1"),
        )
        template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
