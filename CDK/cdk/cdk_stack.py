from aws_cdk import (
    Duration,
    aws_iam as iam,
    aws_s3 as s3,
    Stack,
    aws_glue as glue,
    aws_glue_alpha as glue2,
    aws_lambda as lambda_,
    aws_events as events,
    aws_events_targets as targets,
    aws_cloudtrail as cloudtrail
)
from constructs import Construct

class CdkStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        source_bucket_name = 'bah2-final-project'
        source_crawler_name = 'glue-crawler-s3'
        processed_crawler_name = 'glue-crawler-s3-processed'
        glue_db_name = 'glue_crime_db'
        glue_ServiceUrl = 'glue.amazonaws.com'
        glue_role_name = 'glue-crawler-role'
        glue_job_process = 'glue_process_crime_data'
        glue_process_crime_wf = 'glue_process_crime_wf'
        glue_tgr_initial_name = 'glue_crime_initial_tgr'
        lambda_recent_data_name = 'request_recent_crime_data'


        source_crawler_name_neighborhood = 'glue_crawler_neighborhood'
        glue_db_name_neighborhood = 'glue_neighborhood_db'
        glue_managed_policy_neighborhood = 'arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole'
        glue_ServiceUrl_neighborhood = 'glue.amazonaws.com'
        glue_role_name_neighborhood = 'glue_crawler_neighborhood_role'


        #s3 bucket
        # source_bucket = s3.Bucket(self, source_bucket_name, versioned=False, bucket_name=source_bucket_name, event_bridge_enabled=True)
        source_bucket = s3.Bucket.from_bucket_arn(self, source_bucket_name, bucket_arn='arn:aws:s3:::bah2-final-project')
        #cfnBucket.addPropertyOverride('NotificationConfiguration.EventBridgeConfiguration.EventBridgeEnabled', true);

        event_api_req = events.Rule(self, 'event-api-req', 
            schedule=events.Schedule.expression('cron(0 10 ? * MON-FRI *)'))

        requestLayer = lambda_.LayerVersion.from_layer_version_arn(self, 'layer-request', 'arn:aws:lambda:us-east-1:770693421928:layer:Klayers-p39-requests:2')
        
        lambda_get_data = lambda_.Function(self, lambda_recent_data_name,
            function_name=lambda_recent_data_name,
            runtime=lambda_.Runtime.PYTHON_3_9,
            handler="get_data_api.request_recent_crime_data",
            timeout=Duration.seconds(180),
            code=lambda_.Code.from_bucket(bucket=source_bucket, key='scripts/get_data_api.zip'),
            layers=[requestLayer]
        )
        lambda_get_data.role.add_managed_policy(iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AWSGlueServiceRole'))

        event_api_req.add_target(targets.LambdaFunction(lambda_get_data))

        #create glue db
        glue_db = glue2.Database(self, glue_db_name,
            database_name=glue_db_name
        )

        #IAM role for Glue
        glue_role = iam.Role(self, glue_role_name,
            assumed_by=iam.ServicePrincipal(glue_ServiceUrl),
            role_name=glue_role_name)
        
        #add CloudWatch role so crawler can write logs
        glue_role.add_managed_policy(iam.ManagedPolicy.from_aws_managed_policy_name('CloudWatchLogsFullAccess'))
        glue_role.add_managed_policy(iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AWSGlueServiceRole'))

        #Crawler        
        glue_source_crawler = glue.CfnCrawler(self, source_crawler_name,
            name=source_crawler_name,
            role=glue_role.role_name,
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[glue.CfnCrawler.S3TargetProperty(
                    path=f's3://{source_bucket_name}/input/'
                )]
            ),
            database_name=glue_db.database_name,
            schema_change_policy = glue.CfnCrawler.SchemaChangePolicyProperty(
                delete_behavior="DEPRECATE_IN_DATABASE",
                update_behavior="UPDATE_IN_DATABASE"
            ),
            configuration='{"Version": 1.0,"Grouping": {"TableGroupingPolicy": "CombineCompatibleSchemas" }}'
        )

        # Create Crawlers for Neighborhood code
        
        #create glue db
        glue_db_neighborhood = glue2.Database(self, glue_db_name_neighborhood,
            database_name=glue_db_name_neighborhood
        )

        #Crawler        
        glue_source_crawler_neighborhood = glue.CfnCrawler(self, source_crawler_name_neighborhood,
            name=source_crawler_name_neighborhood,
            role=glue_role.role_name,
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[glue.CfnCrawler.S3TargetProperty(
                    path=f's3://{source_bucket_name}/supporting/'
                )]
            ),
            database_name=glue_db_neighborhood.database_name,
            schema_change_policy = glue.CfnCrawler.SchemaChangePolicyProperty(
                delete_behavior="DEPRECATE_IN_DATABASE",
                update_behavior="UPDATE_IN_DATABASE"
            )
        )

        

        glue_processed_crawler = glue.CfnCrawler(self, processed_crawler_name,
            name=processed_crawler_name,
            role=glue_role.role_name,
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[glue.CfnCrawler.S3TargetProperty(
                    path=f's3://{source_bucket_name}/processed/'
                )]
            ),
            database_name=glue_db.database_name,
            schema_change_policy = glue.CfnCrawler.SchemaChangePolicyProperty(
                delete_behavior="DEPRECATE_IN_DATABASE",
                update_behavior="UPDATE_IN_DATABASE"
            ),
            configuration='{"Version": 1.0,"Grouping": {"TableGroupingPolicy": "CombineCompatibleSchemas" }}'
        )

        glue_job_process_data = glue.CfnJob(self, glue_job_process,
            command=glue.CfnJob.JobCommandProperty(
                name='glueetl',
                python_version='3',
                script_location=f's3://{source_bucket_name}/scripts/process_crime_data.py'
            ),
            role='arn:aws:iam::896639149083:role/AWSGlueServiceRoleDefault',
            connections=None,
            execution_property=glue.CfnJob.ExecutionPropertyProperty(
                max_concurrent_runs=1
            ),
            default_arguments={
                "--job-bookmark-option":"job-bookmark-enable"
            },
            glue_version='3.0',
            max_retries=1,
            name=glue_job_process,
            notification_property=None,
            number_of_workers=10,
            timeout=2880,
            worker_type='G.1X'
        )

        # Glue WF
        glue_crime_workflow = glue.CfnWorkflow(self, glue_process_crime_wf,
            name=glue_process_crime_wf,
        )

        # trigger from EventBidge when s3:PutItem in input folder or when lambda completes
        glue_initial_tgr = glue.CfnTrigger(self, glue_tgr_initial_name,
            name=glue_tgr_initial_name,
            #type='SCHEDULED',ON_DEMAND
            type='EVENT',
            actions=[glue.CfnTrigger.ActionProperty(
                # crawler builds schema on input folder
                crawler_name=glue_source_crawler.name
            )],
            #schedule='cron(0 12 ? * MON-FRI *)',
            workflow_name=glue_crime_workflow.name
        )

        # trigger job on input cawler success
        glue_input_crawler_tgr = glue.CfnTrigger(self, 'glue_input_crawler_tgr',
            name='glue_input_crawler_tgr',
            type='CONDITIONAL',
            start_on_creation=True,
            actions=[glue.CfnTrigger.ActionProperty(
                # job process file
                job_name=glue_job_process_data.name
            )],
            predicate=glue.CfnTrigger.PredicateProperty(
                conditions=[glue.CfnTrigger.ConditionProperty(
                    crawler_name=glue_source_crawler.name,
                    crawl_state='SUCCEEDED',
                    logical_operator='EQUALS'
                )]
            ),
            workflow_name=glue_crime_workflow.name
        )

        # trigger on job success
        glue_processed_crawler_tgr = glue.CfnTrigger(self, 'glue_processed_crawler_tgr',
            name='glue_processed_crawler_tgr',
            type='CONDITIONAL',
            start_on_creation=True,
            actions=[glue.CfnTrigger.ActionProperty(
                # crawler runs on processed folder
                crawler_name=glue_processed_crawler.name
            )],
            predicate=glue.CfnTrigger.PredicateProperty(
                conditions=[glue.CfnTrigger.ConditionProperty(
                    job_name=glue_job_process_data.name,
                    state='SUCCEEDED',
                    logical_operator='EQUALS'
                )]
            ),
            workflow_name=glue_crime_workflow.name
        )

        event_process_source_file = events.CfnRule(self, 'event_crime_wf_tgr',
            # event_pattern=events.EventPattern(
            #     source = ["aws.s3"],
            #     detail_type = ["Object Created"],
            #     detail = {
            #         "bucket": {
            #             "name": ["bah2-final-project"]
            #         },
            #         "object": {
            #             "key": [{"prefix":"input/"}]
            #         }
            #     }
            # ),
            #try to run via cloudtrail events
            event_pattern={
                "source": ["aws.s3"],
                "detail-type": ["AWS API Call via CloudTrail"],
                "detail": {
                    "eventSource": ["s3.amazonaws.com"],
                    "eventName": ["PutObject", "CompleteMultipartUpload", "CopyObject", "RestoreObject"],
                    "requestParameters": {
                        "bucketName": ["bah2-final-project"],
                        "key": [{"prefix":"input/"}]
                    }
                }
            },
            targets=[events.CfnRule.TargetProperty(
                arn=f'arn:aws:glue:us-east-1:896639149083:workflow/{glue_crime_workflow.name}',
                # arn = glue_crime_workflow.get_att('arn').to_string(),
                id=glue_process_crime_wf,
                role_arn='arn:aws:iam::896639149083:role/service-role/Amazon_EventBridge_Invoke_Glue_143070630'
            )]
        )

        s3_trail = cloudtrail.Trail(self, "s3_trail",
            enable_file_validation=False,
            
        )
        s3_trail.add_s3_event_selector(s3_selector=[
            cloudtrail.S3EventSelector(
                bucket=source_bucket, 
                object_prefix='input/'                
            )],
            read_write_type=cloudtrail.ReadWriteType.WRITE_ONLY,
            include_management_events=False
        )
        
        #add wf dependencies
        glue_initial_tgr.add_depends_on(glue_crime_workflow)
        glue_initial_tgr.add_depends_on(glue_source_crawler)
        glue_input_crawler_tgr.add_depends_on(glue_crime_workflow)
        glue_input_crawler_tgr.add_depends_on(glue_job_process_data)
        glue_input_crawler_tgr.add_depends_on(glue_source_crawler)
        glue_processed_crawler_tgr.add_depends_on(glue_crime_workflow)
        glue_processed_crawler_tgr.add_depends_on(glue_processed_crawler)
        glue_processed_crawler_tgr.add_depends_on(glue_job_process_data)

        #Grant read/write access to source bucket
        source_bucket.grant_read(glue_role)
        source_bucket.grant_write(lambda_get_data)