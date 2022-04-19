import boto3
from botocore.exceptions import ClientError
from datetime import datetime, timedelta
import requests

def request_recent_crime_data(event, context):
    '''
    Will call API endpoint to find latest data based on previous max ID saved in DDB
    '''
    #Find date from 7 days ago
    delta = timedelta(days=7)
    target = (datetime.now() - delta).strftime('%Y-%m-%d')
    response = requests.get(f'https://data.cityofchicago.org/resource/ijzp-q8t2.csv?$where=updated_on > "{target}T00:00:00.000"')
    if response.ok:
        response_text = response.text
        if len(response_text) > 250:
            #data has been returned, save into s3
            s3_client = boto3.client('s3')
            try:
                response = s3_client.put_object(
                    Body=response_text,
                    Bucket='bah2-final-project',
                    Key=f'input/recent_source_data_{target}.csv',
                )
            except ClientError as e:
                print(e)