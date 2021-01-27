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
s3 = boto3.resource(
    's3',
    aws_access_key_id=credentials['AccessKeyId'],
    aws_secret_access_key=credentials['SecretAccessKey'],
    aws_session_token=credentials['SessionToken'],
)

####################################
#----Get data for sign-up trend----#
####################################

bucket = 'playfab-events-processing' # already created on S3
key = 'clean_data_admin_dash/quest_signups.csv'

obj = s3.Object(bucket, key)
body = obj.get()['Body'].read()
df = pd.read_csv(BytesIO(body))

# # temporary UNSAFE alternative to above
# df = pd.read_csv('https://playfab-events-processing.s3.us-east-2.amazonaws.com/clean_data_admin_dash/data.csv')

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

supd['7 day avg'] = supd['sign-up count'].rolling(7).mean()

##########################################
#----User activity mapbox scatterplot----#
##########################################

bucket = 'playfab-events-processing'
key = 'clean_data_admin_dash/map_data.csv'

obj = s3.Object(bucket, key)
body = obj.get()['Body'].read()
df = pd.read_csv(BytesIO(body))

df['longitude'] = df['longitude'].astype(float)
df['latitude'] = df['latitude'].astype(float)

# Some collected location data not properly collected
# If city is not listed, usually lat lon coords are in the middle of nowhere
df = df[df['city']!=''].drop_duplicates()

# Exclude Frank's activity
df = df[df['entityid'] != 'BFF17905F1991B38']

# Specifically want to track unique locations reported by active users
# Active users = those that signed up for at least 1 quest, regardless if they ultimately bailed
df = df.drop_duplicates(subset=['entityid','latitude','longitude'])

# frequency = # of active users that were ever reported at that location
df_map = df.groupby(['latitude','longitude','city']).size().to_frame('frequency').reset_index()

########################
#----Dash app setup----#
########################

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

# Sign-up trend line
fig_sups = px.line(supd, x='date', y=['sign-up count','7 day avg'])

fig_map = px.scatter_mapbox(df_map, 
    lat='latitude', 
    lon='longitude', 
    color='frequency', 
    size='frequency', 
    color_continuous_scale='Burg'
)

fig_map.update_layout(mapbox_style="carto-positron", mapbox_center_lon=-74, mapbox_center_lat=40.7)

app.layout = html.Div(children=[

    html.H1(children='Charity Quest Administrator Dashboard'),

    html.Div([

        html.Div([
            html.H3('Daily trend in quest signups'),
            dcc.Graph(id='graph', figure=fig_sups)
        ], className='seven columns'),

        html.Div([
            html.H3('Users signed up by day'),
            dash_table.DataTable(
                id='table',
                columns=[{"name": i, "id": i} for i in tabd.columns]
            )
        ], className='five columns')

    ], className='row'),

    html.Div([
        html.H3('Active users scatter map'),
        html.H5('Active users = signed up for at least one quest (regardless if they ultimately bailed)'),
        html.H5('Frequency = number of active users that have logged-in at given location'),
        dcc.Graph(id='scattermap', figure=fig_map)
    ], className = 'row')

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


application = app.server

if __name__ == '__main__':
    application.run(debug=False, port=8080)
