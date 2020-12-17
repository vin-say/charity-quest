import pandas as pd
import boto3
from io import StringIO
import time

##############################
#----Function definitions----#
##############################
# ref: https://stackoverflow.com/questions/52026405/how-to-create-dataframe-from-aws-athena-using-boto3-get-query-results-method

def results_to_df(results):

    columns = [
        col['Label']
        for col in results['ResultSet']['ResultSetMetadata']['ColumnInfo']
    ]

    listed_results = []
    for res in results['ResultSet']['Rows'][1:]:
        values = []
        for field in res['Data']:
            try:
                values.append(list(field.values())[0]) 
            except:
                values.append(list(' '))

        listed_results.append(
            dict(zip(columns, values))
        )

    return listed_results

#####################
#----Import Data----#
#####################

client = boto3.client('athena')

QueryString = '''
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

queryStart = client.start_query_execution(
    QueryString = QueryString,
    QueryExecutionContext = {
        'Database': 'playfab_events'
    },
    ResultConfiguration = {
        'OutputLocation': 's3://playfab-events-processing/athena-query-results/boto-temp-outputs'
    }
)

# regularly check to see if query has completed before trying to get output
status = 'QUEUED' 
while status in ['RUNNING', 'QUEUED']:
    time.sleep(5)
    status = client.get_query_execution(QueryExecutionId = queryStart['QueryExecutionId'])['QueryExecution']['Status']['State']

response = client.get_query_results(QueryExecutionId = queryStart['QueryExecutionId'])

# convert output to data frame
df = pd.DataFrame(results_to_df(response))

# save in CSV format to S3, first deleting the previous file

client = boto3.client('s3')

bucket = 'playfab-events-processing' # already created on S3
key = 'clean_data_admin_dash/data.csv'

client.delete_object(Bucket=bucket, Key=key)

csv_buffer = StringIO()
df.to_csv(csv_buffer, index=False)
s3_resource = boto3.resource('s3')
s3_resource.Object(bucket, key).put(Body=csv_buffer.getvalue())
