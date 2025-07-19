"""
CDK stack definition for the Stay Event Adapter Proof of Concept.
Sets up Lambda functions, queues, alarms, secrets, and supporting resources.
"""

import os
from aws_cdk import (
    Stack,
    aws_lambda as lambda_fn,
    aws_sqs as sqs,
    aws_sns as sns,
    aws_dynamodb as dynamodb,
    aws_cloudwatch as cw,
    aws_cloudwatch_actions as cw_actions,
    aws_sns_subscriptions as sns_subs,
    aws_logs as logs,
    aws_secretsmanager as secretsmanager,
    aws_ec2 as ec2,
    CfnOutput,
    Duration,
    RemovalPolicy,
)
from aws_cdk.aws_lambda_event_sources import SqsEventSource
from constructs import Construct

class StayEventAdapterPocStack(Stack):  # pylint: disable=too-few-public-methods
    """
    Defines the CDK stack for the Stay Event Adapter POC.
    Includes SNS topics, SQS queues, Lambda functions, CloudWatch alarms,
    and Redshift secret configuration.
    """
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:  # pylint: disable=too-many-locals
        super().__init__(scope,
                        construct_id,
                        **kwargs)

        base_dir = os.path.join(os.path.dirname(__file__), "..", "lambdas")
        adapter_lambda_path = os.path.join(base_dir, "stay_event_adapter")
        test_lambda_path = os.path.join(base_dir, "test_subscriber")

        # VPC and subnet config
        vpc = ec2.Vpc.from_lookup(self, "LambdaVPC", vpc_id="vpc-0464ad6022ebb355c")

        subnets = ec2.SubnetSelection(subnets=[
            ec2.Subnet.from_subnet_id(self, "SubnetA", "subnet-0b351ffc6d0e80b7a"),  # us-west-2a
            ec2.Subnet.from_subnet_id(self, "SubnetB", "subnet-0de5d59f7809d026b"),  # us-west-2b
        ])

        security_group = ec2.SecurityGroup.from_security_group_id(
            self, "LambdaSG", "sg-0c6b6f611ff33a192"
        )

        stay_completed_sns_topic = sns.Topic(self, "StayCompletedTopic",
            topic_name="poc-stay-event"
        )

        CfnOutput(self, "StayCompletedTopicArnOutput",
            value=stay_completed_sns_topic.topic_arn,
            export_name="StayCompletedTopicArn"
        )

        dlq_alarm_topic = sns.Topic(self, "DLQAlarmNotificationTopic",
            topic_name="stay-event-dlq-alarm-notifications"
        )

        dlq_alarm_topic.add_subscription(
            sns_subs.EmailSubscription("dayna.blackwell@bwh.com")
        )

        dlq = sqs.Queue(self, "StayEventDLQ",
            queue_name="poc-stay-event-dlq",
            retention_period=Duration.days(14)
        )

        dlq_alarm = cw.Alarm(self, "DLQMessageAlarm",
            alarm_name="StayEventDLQHasMessages",
            metric=dlq.metric_approximate_number_of_messages_visible(),
            threshold=0,
            evaluation_periods=1,
            comparison_operator=cw.ComparisonOperator.GREATER_THAN_THRESHOLD,
            treat_missing_data=cw.TreatMissingData.NOT_BREACHING,
            alarm_description="Dead letter queue has messages",
        )

        dlq_alarm.add_alarm_action(cw_actions.SnsAction(dlq_alarm_topic))

        dedup_table = dynamodb.Table(
            self, "StayEventDedupTable",
            partition_key=dynamodb.Attribute(
                name="EventHash",
                type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            time_to_live_attribute="TTL",
            removal_policy=RemovalPolicy.DESTROY
        )

        data_available_queue = sqs.Queue(
            self, "DataAvailableQueue",
            queue_name="stay-event-data-available",
            retention_period=Duration.hours(2),
            visibility_timeout=Duration.seconds(120)
        )

        redshift_secret_arn = (
            "arn:aws:secretsmanager:us-west-2:456776821050:secret:"
            "promo-engine/redshift-credentials-VYpjGO"
        )

        redshift_secret = secretsmanager.Secret.from_secret_complete_arn(
            self,
            "RedshiftCredentialsSecret",
            redshift_secret_arn,
        )

        data_services_layer = lambda_fn.LayerVersion(
            self, "DataServicesLayer",
            layer_version_name="data-services-layer",
            compatible_runtimes=[lambda_fn.Runtime.PYTHON_3_13],
            code=lambda_fn.Code.from_asset("lambda-layers/data-services-layer.zip"),
            removal_policy=RemovalPolicy.DESTROY
        )

        stay_event_adapter_lambda = lambda_fn.Function(
            self, "StayEventAdapterLambda",
            runtime=lambda_fn.Runtime.PYTHON_3_13,
            handler="handler.handler",
            code=lambda_fn.Code.from_asset(adapter_lambda_path),
            timeout=Duration.seconds(90),
            tracing=lambda_fn.Tracing.ACTIVE,
            dead_letter_queue=dlq,
            vpc=vpc,
            vpc_subnets=subnets,
            security_groups=[security_group],
            layers=[data_services_layer],
            environment={
                "SNS_TOPIC_ARN": stay_completed_sns_topic.topic_arn,
                "DEDUP_TABLE_NAME": dedup_table.table_name,
                "REDSHIFT_SECRET_ARN": redshift_secret.secret_arn,
            },
        )

        stay_event_adapter_lambda.add_event_source(
            SqsEventSource(data_available_queue)
        )

        test_subscriber_lambda = lambda_fn.Function(
            self, "TestSnsSubscriberLambda",
            runtime=lambda_fn.Runtime.PYTHON_3_13,
            handler="handler.handler",
            code=lambda_fn.Code.from_asset(test_lambda_path),
            tracing=lambda_fn.Tracing.ACTIVE,
        )

        logs.LogGroup(self, "StayEventAdapterLambdaLogGroup",
            log_group_name=f"/aws/lambda/{stay_event_adapter_lambda.function_name}",
            removal_policy=RemovalPolicy.DESTROY,
        )

        logs.LogGroup(self, "TestSnsSubscriberLambdaLogGroup",
            log_group_name=f"/aws/lambda/{test_subscriber_lambda.function_name}",
            removal_policy=RemovalPolicy.DESTROY,
        )

        stay_completed_sns_topic.add_subscription(
            sns_subs.LambdaSubscription(test_subscriber_lambda)
        )

        redshift_secret.grant_read(stay_event_adapter_lambda)
        data_available_queue.grant_consume_messages(stay_event_adapter_lambda)
        stay_completed_sns_topic.grant_publish(stay_event_adapter_lambda)
        dedup_table.grant_read_write_data(stay_event_adapter_lambda)

        ec2.GatewayVpcEndpoint(self, "DynamoDbVpcEndpoint",
            service=ec2.GatewayVpcEndpointAwsService.DYNAMODB,
            vpc=vpc,
        )

        ec2.InterfaceVpcEndpoint(self, "SnsVpcEndpoint",
            service=ec2.InterfaceVpcEndpointAwsService.SNS,
            vpc=vpc,
            subnets=subnets,
            security_groups=[security_group]
        )
