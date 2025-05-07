import os
import random
import string

from aws_cdk import (
    Stack,
    aws_lambda as lambda_fn,
    aws_s3 as s3,
    aws_s3_notifications as s3n,
    aws_sns as sns,
    aws_dynamodb as dynamodb,
    aws_sqs as sqs,
    aws_cloudwatch as cw,
    aws_cloudwatch_actions as cw_actions,
    aws_sns_subscriptions as sns_subs,
    aws_logs as logs,
    CfnOutput,
    Duration,
    Environment,
    RemovalPolicy
)
from constructs import Construct

class StayEventAdapterPocStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, 
                        construct_id, 
                        env=Environment(
                            account="456776821050",
                            region="us-west-2"
                        ),**kwargs
        )
        #
        random_suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=6))
        bucket_name = f"poc-stay-event-adapter-bucket-{random_suffix}"
        
        lambda_code_path = os.path.join(os.path.dirname(__file__), "..", "build")

        # StayCompleted SNS Topic
        stay_completed_sns_topic = sns.Topic(self, "StayCompletedTopic",
            topic_name="poc-stay-event"
        )

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

        # S3 bucket for Stay data
        data_bucket = s3.Bucket(self, "DataBucket",
            bucket_name=bucket_name,
            public_read_access=False,
            auto_delete_objects=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.DESTROY
        )

        # (Dev only) Output the bucket name for deployment script
        CfnOutput(self, "DataBucketNameOutput",
            value=data_bucket.bucket_name,
            description="The name of the data S3 bucket",
            export_name="StayEventAdapterDataBucketName"
        )

        # Lambda function for processing
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
                "BUCKET_NAME": data_bucket.bucket_name,
                "DEDUP_TABLE_NAME": dedup_table.table_name
            }
        )

        # Attach S3 event notification to Stay Adapter Lambda
        notification = s3n.LambdaDestination(stay_event_adapter_lambda)
        data_bucket.add_event_notification(s3.EventType.OBJECT_CREATED, notification)

        # Lambda function for SNS subscription
        test_subscriber_lambda = lambda_fn.Function(
            self, "TestSnsSubscriberLambda",
            runtime=lambda_fn.Runtime.PYTHON_3_13,
            handler="test_subscriber.handler.handler",
            code=lambda_fn.Code.from_asset(lambda_code_path),
            tracing=lambda_fn.Tracing.ACTIVE,
        )

        # Dev only, allows logs to be destroyed with the stack
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

        # Grant permissions
        data_bucket.grant_read(stay_event_adapter_lambda)
        stay_completed_sns_topic.grant_publish(stay_event_adapter_lambda)
        dedup_table.grant_read_write_data(stay_event_adapter_lambda)