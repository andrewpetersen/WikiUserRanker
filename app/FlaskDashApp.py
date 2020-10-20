import flask
import pandas as pd
import plotly.express as px  # (version 4.7.0)
import plotly.graph_objects as go
import os
import psycopg2

import dash  # (version 1.12.0) pip install dash
import dash_core_components as dcc
import dash_html_components as html
import dash_table
from dash.dependencies import Input, Output
from plotly.subplots import make_subplots

server = flask.Flask(__name__)

@server.route('/')
def index():
    return 'Hello Flask app'

app = dash.Dash(__name__,server=server,routes_pathname_prefix='/dash/')

# ------------------------------------------------------------------------------
# Import and clean data (importing csv into pandas)
df = pd.read_csv("intro_bees.csv")

df = df.groupby(['State', 'ANSI', 'Affected by', 'Year', 'state_code'])[['Pct of Colonies Impacted']].mean()
df.reset_index(inplace=True)
print(df[:5])

df2 = pd.read_csv("SampleUsers.csv")
print(df2)

print("Start PG Attempt")
postgresUser = os.environ['POSTGRES_USER']
postgresPass = os.environ['POSTGRES_PASS']
print("Loaded user and pass")
conn = psycopg2.connect(
   # host="54.151.4.229",
    host="10.0.0.11",
    port="5442",
    database="cluster_output",
    user=postgresUser,
    password=postgresPass)
print("Connected to database")
cur = conn.cursor()
print('PostgreSQL database version:')
cur.execute('SELECT version()')
db_version = cur.fetchone()
print(db_version)
cur.execute('SELECT * FROM ContributorScores')
contributorScores = cur.fetchall()
cur.close()

contributorScoresDF = pd.DataFrame.from_records(contributorScores, columns=['User','Sum','Avg','Count','isRegistered'])
print(contributorScoresDF)

tableFig2 = go.Table(
             header=dict(
               values=["User", "Sum", "Avg",
                       "Count", "isRegistered"],
               font=dict(size=10),
               align="left"
             ),
             cells=dict(
               values=[contributorScoresDF[k].tolist() for k in contributorScoresDF.columns[1:]],
               align = "left")
           )
ddff = contributorScoresDF
tableFig = go.Figure(data=[go.Table(
                                    header=dict(values=list(contributorScoresDF.columns),
                                    fill_color='paleturquoise',
                                    align='left'),
                           cells=dict(values=[ddff.User, ddff.Sum, ddff.Avg, ddff.Count, ddff.isRegistered],
                                      fill_color='lavender',
                                      align='left'))
])
#for row in mobile_records:
    

# ------------------------------------------------------------------------------
# App layout
app.layout = html.Div([

    html.H1("WikiRanks", style={'text-align': 'center'}),

    dcc.Dropdown(id="slct_year",
                 options=[
                     {"label": "Sum", "value": "Sum"},
                     {"label": "Avg", "value": "Avg"},
                     {"label": "Count", "value": "Count"},
                     {"label": "2018", "value": "Extra"}],
                 multi=False,
                 value="Sum",
                 style={'width': "40%"}
                 ),

    html.Div(id='HistogramContainer', children=[]),
    html.Br(),
    dcc.Graph(id='UserHist', figure={}),
    html.Div(id='TableContainer', children=[]),
    html.Br(),
    dcc.Graph(id='UserTable', figure=tableFig)
])

# ------------------------------------------------------------------------------
# Connect the Plotly graphs with Dash Components
@app.callback(
    [Output(component_id='HistogramContainer', component_property='children'),
     Output(component_id='UserHist', component_property='figure')],
    [Input(component_id='slct_year', component_property='value')]
)
def update_graph(option_slctd):
    print(option_slctd)
    print(type(option_slctd))

    print('Updating Graph')
    print("Updating data frame from server")
    print("Connecting to Postgresql Server")
    conn = psycopg2.connect(
        host="10.0.0.11",
        port="5442",
        database="cluster_output",
        user=postgresUser,
        password=postgresPass)
    print("Connected to database")
    cur = conn.cursor()
    cur.execute('SELECT * FROM ContributorScores')
    contributorScores = cur.fetchall()
    cur.close()
    contributorScoresDF = pd.DataFrame.from_records(contributorScores, columns=['User','Sum','Avg','Count','isRegistered'])

    container = "Sorting By: {}".format(option_slctd)

    dff = df.copy()
    dff = dff[dff["Year"] == option_slctd]
    dff = dff[dff["Affected by"] == "Varroa_mites"]
    dff2 = df2.copy()

    # Plotly Express Histogram
    fig = px.histogram(contributorScoresDF, x=option_slctd, color="isRegistered", marginal="rug",hover_data=contributorScoresDF.columns)

#    fig.add_trace(
      # dash_table.DataTable(id='table',
      #                      columns=[{"name": i, "id": i} for i in contributorScoresDF.columns],
      #                      data=contributorScoresDF.to_dict('records'),
      # ),
 #       row=2, col=1
 #   )
    # Plotly Express
    #fig = px.choropleth(
    #    data_frame=dff,
    #    locationmode='USA-states',
    #    locations='state_code',
    #    scope="usa",
    #    color='Pct of Colonies Impacted',
    #    hover_data=['State', 'Pct of Colonies Impacted'],
    #    color_continuous_scale=px.colors.sequential.YlOrRd,
    #    labels={'Pct of Colonies Impacted': '% of Bee Colonies'},
    #    template='plotly_dark'
    #)

    # Plotly Graph Objects (GO)
    # fig = go.Figure(
    #     data=[go.Choropleth(
    #         locationmode='USA-states',
    #         locations=dff['state_code'],
    #         z=dff["Pct of Colonies Impacted"].astype(float),
    #         colorscale='Reds',
    #     )]
    # )
    #
    # fig.update_layout(
    #     title_text="Bees Affected by Mites in the USA",
    #     title_xanchor="center",
    #     title_font=dict(size=24),
    #     title_x=0.5,
    #     geo=dict(scope='usa'),
    # )

    return container, fig


# ------------------------------------------------------------------------------
if __name__ == '__main__':
    app.run_server(debug=False)
