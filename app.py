# -*- coding: utf-8 -*-

# Run this app with `python app.py` and
# visit http://127.0.0.1:8050/ in your web browser.

import pandas as pd
import boto3
from io import BytesIO
import matplotlib.pyplot as plt
import plotly.express as px
import plotly.graph_objects as go
import time

import dash
from dash.dependencies import Input, Output
import dash_table
import dash_core_components as dcc
import dash_html_components as html

# ##############################
# #----Function definitions----#
# ##############################
# # ref: https://stackoverflow.com/questions/52026405/how-to-create-dataframe-from-aws-athena-using-boto3-get-query-results-method

# def results_to_df(results):

#     columns = [
#         col['Label']
#         for col in results['ResultSet']['ResultSetMetadata']['ColumnInfo']
#     ]

#     listed_results = []
#     for res in results['ResultSet']['Rows'][1:]:
#         values = []
#         for field in res['Data']:
#             try:
#                 values.append(list(field.values())[0]) 
#             except:
#                 values.append(list(' '))

#         listed_results.append(
#             dict(zip(columns, values))
#         )

#     return listed_results

# #####################
# #----Import Data----#
# #####################

# client = boto3.client('athena')

# QueryString = '''
#     WITH usrs AS (
#     SELECT DISTINCT entityid, username 
#     FROM playfab_events.trans_player_linked_account
#     ),

#     times AS (
#     SELECT DISTINCT entityid, timestamp
#     FROM playfab_events.trans_player_inventory_item_added
#     )

#     SELECT usrs.username, usrs.entityid, times.timestamp
#     FROM times
#     JOIN usrs ON times.entityid = usrs.entityid
#     ORDER BY timestamp
# '''

# queryStart = client.start_query_execution(
#     QueryString = QueryString,
#     QueryExecutionContext = {
#         'Database': 'playfab_events'
#     },
#     ResultConfiguration = {
#         'OutputLocation': 's3://playfab-events-processing/athena-query-results/boto-temp-outputs'
#     }
# )

# # regularly check to see if query has completed before trying to get output
# status = 'QUEUED' 
# while status in ['RUNNING', 'QUEUED']:
#     time.sleep(5)
#     status = client.get_query_execution(QueryExecutionId = queryStart['QueryExecutionId'])['QueryExecution']['Status']['State']

# response = client.get_query_results(QueryExecutionId = queryStart['QueryExecutionId'])

# # convert output to data frame
# df = pd.DataFrame(results_to_df(response))

bucket = 'playfab-events-processing' # already created on S3
key = 'clean_data_admin_dash/data.csv'

s3 = boto3.resource('s3')
obj = s3.Object(bucket, key)
body = obj.get()['Body'].read()
df = pd.read_csv(BytesIO(body))

# convert timestamp from string to datetime type
df['timestamp'] = pd.to_datetime(df['timestamp'])
df['timestamp_est'] = df['timestamp'].dt.tz_convert('US/Eastern')
df['date'] = df['timestamp_est'].dt.date.astype('datetime64') # very important that 'date' data types for tabd and supd match!

# define table that will be displayed on dashboard
tabd = df[['username', 'entityid', 'timestamp_est', 'timestamp', 'date']] # tabd = table data
tabd['timestamp'] = tabd['timestamp'].dt.round('s').dt.time
tabd['timestamp_est'] = tabd['timestamp_est'].dt.round('s').dt.time

# create data frame listing number of signups grouped by date
supd = tabd.groupby(['date']).size() #supd = sign-ups per day

# some dates don't have any signups. fill in missing dates
min_date = min(df['date'])
max_date = max(df['date'])
idx = pd.date_range(min_date, max_date)

supd.index = pd.DatetimeIndex(supd.index)
supd = supd.reindex(idx, fill_value=0)

supd = supd.reset_index().rename(columns={'index': 'date', 0:'sign-up count'})



########################
#----Dash app setup----#
########################

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

fig = px.line(supd, x='date', y='sign-up count')

app.layout = html.Div(children=[

    html.H1(children='Charity Quest Administrator Dashboard'),

    html.Div([

        html.Div([
            html.H3('Daily trend in quest signups'),
            dcc.Graph(id='graph', figure=fig)
        ], className='seven columns'),

        html.Div([
            html.H3('Users signed up by day'),
            dash_table.DataTable(
                id='table',
                columns=[{"name": i, "id": i} for i in tabd.columns]
            )
        ], className='five columns')

    ], className='row')

])

@app.callback(
    Output('table', 'data'),
    Input('graph', 'clickData'))
def update_table(clickData):
    if clickData is None:
        xval = max(supd['date'])
    else:
        xval = clickData['points'][0]['x']

    dff = tabd[tabd['date'] == xval].copy()
    dff['date'] = dff['date'].astype('str')

    return dff.to_dict('records')

if __name__ == '__main__':
    app.run_server(debug=True)
