# -*- coding: utf-8 -*-

# Run this app with `python app.py` and
# visit http://127.0.0.1:8050/ in your web browser.

import dash
from dash.dependencies import Input, Output
import dash_table
import dash_core_components as dcc
import dash_html_components as html
import plotly.express as px
import pandas as pd

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

# assume you have a "long-form" data frame
# see https://plotly.com/python/px-arguments/ for more options
df = pd.read_csv('testset.csv')

fig = px.line(df, x='date', y='sign-up count')

app.layout = html.Div(children=[
    html.H1(children='Charity Quest Administrator Dashboard'),

    html.Div(children='''
        Daily trend in quest sign-ups.
    '''),

    dcc.Graph(
        id='graph',
        figure=fig
    ),

    dash_table.DataTable(
        id='table',
        columns=[{"name": i, "id": i} for i in df.columns]
    )

])

@app.callback(
    Output('table', 'data'),
    Input('graph', 'clickData'))
def update_table(clickData):
    if clickData is None: 
        xval = max(df['date'])
    else: 
        xval = clickData['points'][0]['x']

    dff = df[df['date'] == xval]
    
    return dff.to_dict('records')

if __name__ == '__main__':
    app.run_server(debug=True)