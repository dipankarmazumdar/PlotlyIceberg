import os, pandas as pd
from pyarrow import flight
import pyarrow as pa
from os import environ
import plotly.express as px
import plotly.graph_objects as go
import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc
from dash import dash_table
from dash.dependencies import Input, Output, State

token = environ.get('token', 'DremioToken')

headers = [
    (b"authorization", f"bearer {token}".encode("utf-8"))
    ]

client = flight.FlightClient('grpc+tls://data.dremio.cloud:443')
options = flight.FlightCallOptions(headers=headers)

sql = '''SELECT * FROM Samples."samples.dremio.com"."NYC-taxi-trips-iceberg"
         LIMIT 100000'''

info = client.get_flight_info(flight.FlightDescriptor.for_command(sql + '-- arrow flight'),options)

reader = client.do_get(info.endpoints[0].ticket, options)

batches = []
while True:
    try:
        
        batch, metadata = reader.read_chunk()
        batches.append(batch)
    except StopIteration:
        break
data = pa.Table.from_batches(batches)
df = data.to_pandas()

# print(df)

df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])
df['date'] = df['pickup_datetime'].dt.date
trip_distance_over_time = df.groupby('date')['trip_distance_mi'].sum().reset_index()

avg_fare_by_passenger_count = df.groupby('passenger_count')['fare_amount'].mean().reset_index()

# Set the color and theme for the charts
colors = {
    'background': '#F3F6FA',
    'text': '#1F2937',
    'chart': '#674883',
    'highlight': '#F7AE4D'
}

# Set the template for the charts
template = go.layout.Template(
    layout=go.Layout(
        plot_bgcolor=colors['background'],
        paper_bgcolor=colors['background'],
        font={'color': colors['text']}
    )
)


app = dash.Dash(__name__)

# Set font
font = "DM Sans"

# Define layout
app.layout = html.Div(
    children=[
        html.Link(rel='stylesheet', href='/assets/styles.css'),  # Link the CSS file
        html.H1("NYC Trips Dashboard", style={"text-align": "center", "margin-top": "30px", "font-family": font, "font-weight": "bold"}),
        html.Div(
            children=[
                dcc.Graph(
                    figure=px.bar(avg_fare_by_passenger_count, x='passenger_count', y='fare_amount', title='Average Fare Amount by Passenger Count')
                     .update_traces(marker_color=colors['chart'])
                .update_layout(template=template, font=dict(family="DM Sans", size=12, color="#1F2937")),
                    config={"displayModeBar": False},
                ),
                dcc.Graph(
                    figure=px.scatter(df, x='trip_distance_mi', y='total_amount', title='Trip Distance vs Total Amount' )
                    .update_traces(marker_color=colors['chart'])
                    .update_layout(template=template, font=dict(family="DM Sans", size=12, color="#1F2937")),
                    config={"displayModeBar": False},
                ),
            ],
            style={"display": "flex", "justify-content": "space-between", "margin-bottom": "30px"}
        ),
        html.Div(
            children=[
                dcc.Graph(
                    figure = px.line(trip_distance_over_time, x='date', y='trip_distance_mi', title='Total Trip Distance Over Time')
                    .update_traces(line_color=colors['chart'])
                    .update_layout(template=template, font=dict(family="DM Sans", size=12, color="#1F2937")),
                    config={"displayModeBar": False},
                ),
                dcc.Graph(
                    figure=px.scatter_matrix(df, dimensions=['trip_distance_mi', 'fare_amount', 'tip_amount', 'total_amount'])
                    .update_traces(marker_color=colors['chart'])
                .update_layout(template=template, font=dict(family="DM Sans", size=12, color="#1F2937")),
                    config={"displayModeBar": False},
                ),
            ],
            style={"display": "flex", "justify-content": "space-between"}
        ),
        html.Div(
            children=[
         dcc.Textarea(
            id='input-query',
            placeholder='Enter SQL query...',
            style={'width': '100%', 'height': '100px', 'margin-bottom': '10px'}
        ),
        
        html.Button('Run Query', id='run-query-button', n_clicks=0),
        
        dash_table.DataTable(
            id='data-table',
            style_table={'overflowX': 'auto'},
            style_cell={'textAlign': 'left'},
        )
            ],
            
        ),

       
        
        # Date table object
        html.Div(id='date-table-container')
    ],
    style={"margin": "auto", "max-width": "1500px"}
)



@app.callback(
    Output('data-table', 'data'),
    Output('data-table', 'columns'),
    Input('run-query-button', 'n_clicks'),
    State('input-query', 'value')
)

def run_query(n_clicks, query):
    if n_clicks > 0 and query:
        token = environ.get('token', 'DremioToken')

        headers = [
        (b"authorization", f"bearer {token}".encode("utf-8"))
            ]

        client = flight.FlightClient('grpc+tls://data.dremio.cloud:443')
        options = flight.FlightCallOptions(headers=headers)

        print(query)

        sql = query

        info = client.get_flight_info(flight.FlightDescriptor.for_command(sql + '-- arrow flight'),options)

        reader = client.do_get(info.endpoints[0].ticket, options)

        batches = []
    
        while True:
            try:
                batch, metadata = reader.read_chunk()
                batches.append(batch)
            except StopIteration:
                break
        data = pa.Table.from_batches(batches)
        df = data.to_pandas()
        data_new = df.to_dict('records')
        columns = [{'name': col, 'id': col} for col in df.columns]
        
        return data_new, columns

    default_data = []
    default_columns = [{'name': 'No Data', 'id': 'nodata'}]
    return default_data, default_columns

# Run the app
if __name__ == "__main__":
    app.run_server(debug=True)
