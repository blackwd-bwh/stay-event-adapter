import os
import random
import string
import json

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
    CfnOutput,
    Duration,
    Environment,
    RemovalPolicy
)
from aws_cdk.aws_lambda_event_sources import SqsEventSource
from constructs import Construct

class StayEventAdapterPocStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope,
                         construct_id,
                         env=Environment(account="456776821050", region="us-west-2"),
                         **kwargs)

        lambda_code_path = os.path.join(os.path.dirname(__file__), "..", "build")

        # StayCompleted SNS Topic
        stay_completed_sns_topic = sns.Topic(self, "StayCompletedTopic",
            topic_name="poc-stay-event"
        )

        # Stores ARN of StayCompleted SNS Topic, for use by Consumers
        CfnOutput(self, "StayCompletedTopicArnOutput",
            value=stay_completed_sns_topic.topic_arn,
            export_name="StayCompletedTopicArn"
        )

        # DLQ Alarm SNS Topic
        dlq_alarm_topic = sns.Topic(self, "DLQAlarmNotificationTopic",
            topic_name="stay-event-dlq-alarm-notifications"
        )

        dlq_alarm_topic.add_subscription(
            sns_subs.EmailSubscription("dayna.blackwell@bwh.com")
        )

        # Dead Letter Queue for failed Lambda (Stay Adapter) invocations
        dlq = sqs.Queue(self, "StayEventDLQ",
            queue_name="poc-stay-event-dlq",
            retention_period=Duration.days(14)
        )

        # CloudWatch alarm for DLQ
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

        # DynamoDB table for deduplication
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

        # SQS queue for new data available notifications
        data_available_queue = sqs.Queue(
            self, "DataAvailableQueue",
            queue_name="stay-event-data-available",
            retention_period=Duration.days(4),
            visibility_timeout=Duration.seconds(120)  # Must be ≥ Lambda timeout
        )

        # Use existing Redshift credentials from Secrets Manager
        redshift_secret = secretsmanager.Secret.from_secret_complete_arn(
            self, "RedshiftCredentialsSecret",
            "arn:aws:secretsmanager:us-west-2:456776821050:secret:promo-engine/redshift-credentials-VYpjGO"
        )

        # Lambda function for processing messages
        stay_event_adapter_lambda = lambda_fn.Function(
            self, "StayEventAdapterLambda",
            runtime=lambda_fn.Runtime.PYTHON_3_13,
            handler="stay_event_adapter.handler.handler",
            code=lambda_fn.Code.from_asset(lambda_code_path),
            timeout=Duration.seconds(90),
            tracing=lambda_fn.Tracing.ACTIVE,
            dead_letter_queue=dlq,
            environment={
                "SNS_TOPIC_ARN": stay_completed_sns_topic.topic_arn,
                "DEDUP_TABLE_NAME": dedup_table.table_name,
                "REDSHIFT_SECRET_ARN": redshift_secret.secret_arn
            }
        )

        # Connect SQS to Lambda
        stay_event_adapter_lambda.add_event_source(
            SqsEventSource(data_available_queue)
        )

        # Lambda function for SNS subscription test
        test_subscriber_lambda = lambda_fn.Function(
            self, "TestSnsSubscriberLambda",
            runtime=lambda_fn.Runtime.PYTHON_3_13,
            handler="test_subscriber.handler.handler",
            code=lambda_fn.Code.from_asset(lambda_code_path),
            tracing=lambda_fn.Tracing.ACTIVE,
        )

        # Log groups for Lambda functions (optional cleanup)
        logs.LogGroup(self, "StayEventAdapterLambdaLogGroup",
            log_group_name=f"/aws/lambda/{stay_event_adapter_lambda.function_name}",
            removal_policy=RemovalPolicy.DESTROY,
        )

        logs.LogGroup(self, "TestSnsSubscriberLambdaLogGroup",
            log_group_name=f"/aws/lambda/{test_subscriber_lambda.function_name}",
            removal_policy=RemovalPolicy.DESTROY,
        )

        # SNS subscription
        stay_completed_sns_topic.add_subscription(
            sns_subs.LambdaSubscription(test_subscriber_lambda)
        )

        # Grant permissions
        redshift_secret.grant_read(stay_event_adapter_lambda)
        data_available_queue.grant_consume_messages(stay_event_adapter_lambda)
        stay_completed_sns_topic.grant_publish(stay_event_adapter_lambda)
        dedup_table.grant_read_write_data(stay_event_adapter_lambda)
