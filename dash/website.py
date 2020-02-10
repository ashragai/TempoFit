import os
import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_table
from dash.dependencies import Output, Input, State
import plotly.graph_objs as go
import redis
import json
import numpy as np

HOST = 
PORT = 5432
client = redis.Redis(host= HOST, port= PORT)

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
server = app.server

hrFig = go.Figure()

app.layout = html.Div(children=[
    html.H1(children='TempoFit: better music, better workout'),

    html.Div(children='''
        Enter a user ID to view latest workout information:
    '''),

    html.Div([dcc.Input(
        placeholder= 'User ID...',
        type = 'number',
        id= 'getUsrInfoBox'),
        html.Button('Go!', id= 'getUsrInfoBtn')
        ]),
    html.Div(dcc.Graph(id= 'hrGraph', figure = hrFig)),
    html.Div(dash_table.DataTable(id= 'songTable',
        columns = [{'name' : 'Artist', 'id' : 'artist'},
        {'name': 'Title', 'id' : 'title'},
        {'name' : 'Duration', 'id' : 'duration'}],
        style_cell_conditional= [{'if' : {'column_id' : 'artist'}, 'width' : '30%'},
        {'if' : {'column_id' : 'title'}, 'width' : '30%'},
        {'if' : {'column_id' : 'duration'}, 'width' : '30%'}],
        data = []))

])

@app.callback(
    [Output('hrGraph', 'figure'),
    Output('songTable', 'data'),
    ],
    [Input('getUsrInfoBtn', 'n_clicks')],
    [State('getUsrInfoBox', 'value')]

)
def update_output(n_clicks, value):
    try:
        usr_info = json.loads(client.get(value))
        t = np.insert(np.cumsum(usr_info['dur']), 0, 0)[:-1]
        t /= 60.
        h = usr_info['heartrate']
        songLen = ["{}:{:02d}".format(int(x/60), int(x - 60*int(x/60))) for x in usr_info['dur']]
        newTbl = [{'artist' : usr_info['artist'][ii], 'title' : usr_info['title'][ii], 'duration' : songLen[ii]} for ii in range(len(usr_info['dur']))]
        newFig = go.Figure(data=[go.Scatter(x= t, y= h)])
        ttl= "User {}: Heartrate vs. Time".format(value)
        newFig.update_layout(title = ttl, xaxis_title= "Time (min)", yaxis_title= "Heartrate (BPM)")
        return newFig, newTbl

    except Exception as e:
        newFig = go.Figure()
        newTbl = []
        return newFig, newTbl


if __name__ == '__main__':
    app.run_server(host='0.0.0.0',debug=True)