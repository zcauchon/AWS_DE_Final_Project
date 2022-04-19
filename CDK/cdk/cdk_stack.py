from aws_cdk import (
    aws_iam as iam,
    aws_s3 as s3,
    Stack,
    aws_glue as glue,
    aws_glue_alpha as glue2,
    aws_lambda as lambda_,
    aws_events as events,
    aws_events_targets as targets,
    aws_lambda_destinations as destinations
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
        lambda_recent_data_name = 'request_recent_crime_data'

        #s3 bucket
        source_bucket = s3.Bucket(self, source_bucket_name, versioned=False, bucket_name=source_bucket_name)

        event_api_req = events.Rule(self, 'event-api-req', 
            schedule=events.Schedule.expression('cron(0 10 ? * MON-FRI *)'))

        requestLayer = lambda_.LayerVersion.from_layer_version_arn(self, 'layer-request', 'arn:aws:lambda:us-east-1:770693421928:layer:Klayers-p39-requests:2')
        
        lambda_get_data = lambda_.Function(self, lambda_recent_data_name,
            function_name=lambda_recent_data_name,
            runtime=lambda_.Runtime.PYTHON_3_9,
            handler="get_data_api.request_recent_crime_data",
            code=lambda_.Code.from_bucket(bucket=source_bucket, key='scripts/get_data_api.zip'),
            layers=[requestLayer]
            #,on_success=destinations.EventBridgeDestination()
        )
        #allow lambda to write EventBirdge on success
        lambda_get_data.role.add_managed_policy(iam.ManagedPolicy.from_aws_managed_policy_name('CloudWatchEventsFullAccess'))

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
            )
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
        glue_initial_tgr = glue.CfnTrigger(self, 'glue_crime_initial_tgr',
            name='glue_crime_initial_tgr',
            type='SCHEDULED',
            start_on_creation=False,
            actions=[glue.CfnTrigger.ActionProperty(
                # crawler builds schema on input folder
                crawler_name=glue_source_crawler.name
            )],
            # for now schedule wiht cron - : change to event driven ASAP
            schedule='cron(0 12 ? * MON-FRI *)',
            workflow_name=glue_crime_workflow.name
        )

        # trigger job on input cawler success
        glue_input_crawler_tgr = glue.CfnTrigger(self, 'glue_input_crawler_tgr',
            name='glue_input_crawler_tgr',
            type='CONDITIONAL',
            start_on_creation=False,
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
            start_on_creation=False,
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

        #make tgrs depend on wf
        glue_initial_tgr.add_depends_on(glue_crime_workflow)
        glue_input_crawler_tgr.add_depends_on(glue_crime_workflow)
        glue_processed_crawler_tgr.add_depends_on(glue_crime_workflow)

        #Grant crawler read/write access to source bucket
        source_bucket.grant_read(glue_role)
        source_bucket.grant_write(lambda_get_data)
        
        #glue job