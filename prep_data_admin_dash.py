import pandas as pd
import boto3
from io import StringIO
import time

##############################
#----Function definitions----#
##############################

def run_athena_query(boto_client, query_string, database, output_location):
    '''Run Athena query and output the result

    param boto_client (Boto3 client): Boto3 Athena client
    param query_string (str): SQL query
    param database (str): Athena database name from where tables originate
    param output_location (str): S3 location where queries are stored

    return query_output: Athena output
    '''

    queryStart = boto_client.start_query_execution(
        QueryString = query_string,
        QueryExecutionContext = {
            'Database': database
        },
        ResultConfiguration = {
            'OutputLocation': output_location
        }
    )

    # regularly check to see if query has completed before trying to get output
    status = 'QUEUED' 
    while status in ['RUNNING', 'QUEUED']:
        time.sleep(5)
        status = boto_client.get_query_execution(QueryExecutionId = queryStart['QueryExecutionId'])['QueryExecution']['Status']['State']

    results_paginator = boto_client.get_paginator('get_query_results')
    results_iter = results_paginator.paginate(QueryExecutionId = queryStart['QueryExecutionId'])

    results = []
    data_list = []
    for results_page in results_iter:
        for row in results_page['ResultSet']['Rows']:
            data_list.append(row['Data'])
    for datum in data_list[0:]:
        results.append([x['VarCharValue'] if 'VarCharValue' in x else '' for x in datum])
    return [tuple(x) for x in results]

def df2csv_S3(boto_client, df, bucket, key):
    '''Run Athena query and output the result, first emptying the file location

    param boto_client (Boto3 client): Boto3 Athena client
    param df (Pandas dataframe): Dataframe to save
    param bucket (str): Name of S3 bucket file will be saved in
    param key (str): File path
    '''

    boto_client.delete_object(Bucket=bucket, Key=key)

    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3_resource = boto3.resource('s3')
    s3_resource.Object(bucket, key).put(Body=csv_buffer.getvalue())

##############################################
#----Assume role to gain temp credentials----#
##############################################

# create an STS client object that represents a live connection to the 
# STS service
sts_client = boto3.client('sts')

# Call the assume_role method of the STSConnection object and pass the role
# ARN and a role session name.
assumed_role_object=sts_client.assume_role(
    RoleArn="arn:aws:iam::373598043715:role/Boto3-Access-Assumed",
    RoleSessionName="AssumeRoleSession1"
)

# From the response that contains the assumed role, get the temporary 
# credentials that can be used to make subsequent API calls
credentials=assumed_role_object['Credentials']

# Use the temporary credentials that AssumeRole returns to make a 
# connection to Amazon S3  
s3_resource=boto3.resource(
    's3',
    aws_access_key_id=credentials['AccessKeyId'],
    aws_secret_access_key=credentials['SecretAccessKey'],
    aws_session_token=credentials['SessionToken'],
)

#####################
#----Import Data----#
#####################

athena_client = boto3.client(
    'athena',
    aws_access_key_id=credentials['AccessKeyId'],
    aws_secret_access_key=credentials['SecretAccessKey'],
    aws_session_token=credentials['SessionToken']
)

database = 'playfab_events'
output_location = 's3://playfab-events-processing/athena-query-results/boto-temp-outputs'

s3_client = boto3.client(
    's3',
    aws_access_key_id=credentials['AccessKeyId'],
    aws_secret_access_key=credentials['SecretAccessKey'],
    aws_session_token=credentials['SessionToken'],
)

bucket = 'playfab-events-processing'

# query definitions
query_quest_signup = '''
    WITH usrs AS (
        SELECT DISTINCT entityid, username 
        FROM playfab_events.trans_player_linked_account
    ),

    times AS (
        SELECT DISTINCT entityid, timestamp
        FROM playfab_events.trans_player_inventory_item_added
    )

    SELECT usrs.username, usrs.entityid, times.timestamp
    FROM times
    JOIN usrs ON times.entityid = usrs.entityid
    ORDER BY timestamp
'''

query_map = '''
    WITH locs AS 
        (SELECT DISTINCT eventid,
            entityid,
            platformusername,
            location.countrycode countrycode,
            location.city city,
            location.latitude latitude,
            location.longitude longitude,
            timestamp
        FROM playfab_events.trans_player_logged_in ), usrs AS 
        (SELECT DISTINCT entityid
        FROM playfab_events.trans_player_inventory_item_added
        WHERE itemid = 'quest_contract'
        GROUP BY  entityid)
        
    SELECT locs.platformusername,
            usrs.entityid,
            locs.countrycode,
            locs.city,
            locs.latitude,
            locs.longitude,
            locs.timestamp
    FROM locs
    JOIN usrs
        ON locs.entityid = usrs.entityid
    ORDER BY timestamp
'''

# run and save queries
response = run_athena_query(athena_client, query_quest_signup, database, output_location)
df = pd.DataFrame(response[1:], columns=response[0])
key = 'clean_data_admin_dash/quest_signups.csv'
df2csv_S3(s3_client, df, bucket, key)

response = run_athena_query(athena_client, query_map, database, output_location)
df = pd.DataFrame(response[1:], columns=response[0])
key = 'clean_data_admin_dash/map_data.csv'
df2csv_S3(s3_client, df, bucket, key)


# update the elastic beanstalk server so Dash app reflects latest data
eb_client = boto3.client(
    'elasticbeanstalk',
    aws_access_key_id=credentials['AccessKeyId'],
    aws_secret_access_key=credentials['SecretAccessKey'],
    aws_session_token=credentials['SessionToken']
)

eb_client.restart_app_server(
    EnvironmentId = 'e-cxdsnkbhk3',
    EnvironmentName = 'CqAdDash-env'
)