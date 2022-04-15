from aws_cdk import (
    aws_iam as iam,
    aws_s3 as s3,
    Stack,
    aws_glue as glue
)
from constructs import Construct

class CdkStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        source_bucket_name = 'bah2-final-project-source'
        source_crawler_name = 'glue-crawler-s3'
        glue_db_name = 'glue-crime-db'
        glue_managed_policy = 'arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole'
        glue_ServiceUrl = 'glue.amazonaws.com'
        glue_role_name = 'glue-crawler-role'

        #s3 bucket
        source_bucket = s3.Bucket(self, source_bucket_name, versioned=False, bucket_name=source_bucket_name)
        
        #create glue db
        # glue_db = glue.CfnDatabase(self, glue_db_name, 
        #     catalog_id='!Ref AWS::AccountId',
        #     database_input=glue.CfnDatabase.DatabaseInputProperty(
        #         create_table_default_permissions=[glue.CfnDatabase.PrincipalPrivilegesProperty(
        #             permissions=['*'],
        #             principal='*'
        #         )],
        #         name=glue_db_name
        #     )
        # )

        #IAM role for Glue
        glue_crawler_role = iam.Role(self, glue_role_name,
            assumed_by=iam.ServicePrincipal(glue_ServiceUrl),
            role_name=glue_role_name)

        #Crawler        
        glue_source_crawler = glue.CfnCrawler(self, source_crawler_name,
            name=source_crawler_name,
            role=glue_crawler_role.role_name,
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[glue.CfnCrawler.S3TargetProperty(
                    path=source_bucket.s3_url_for_object()
                )]
            ),
            database_name=glue_db_name,
            schema_change_policy = glue.CfnCrawler.SchemaChangePolicyProperty(
                delete_behavior="DEPRECATE_IN_DATABASE",
                update_behavior="UPDATE_IN_DATABASE"
            )
        )

        #Grant crawler read/write access to source bucket
        source_bucket.grant_read(glue_crawler_role)