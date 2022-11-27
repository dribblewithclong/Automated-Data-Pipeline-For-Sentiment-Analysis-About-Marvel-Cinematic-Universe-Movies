import pandas as pd
import sqlite3
import sqlalchemy
import numpy as np
import dash
from dash import dcc
from dash import html
from dash.dash_table.Format import Group
from dash import dash_table as dt
from dash.dependencies import Input, Output
import plotly.express as px

#Load dataset from database
conn = sqlite3.connect("/imdb_movies.sqlite")
df = pd.read_sql_query("SELECT * from marvel_movies_reddit_sentiment", conn)
recommendation = pd.read_csv('movies_recommendation.csv',sep='|').set_index('Unnamed: 0')

#App name
app = dash.Dash(__name__)

#App layout
app.layout = html.Div(
    children = [
        html.Div(
            children = [
                html.H1('Sentiment Analysis and Recommendation System About Marvel Cinematic Universe Movies'),
                html.Img(src = r'assets/marvel.jpg', alt = 'image',style = {'width':'20%','height':'20%'})
            ], className = 'container-above'
        ),
        html.Div(
            children = [
                html.Div(
                    children = [
                        html.H2('Sentiment Analysis From Reddit'),
                        html.Div(
                            children = [
                                html.Label('Movie Selection:'),
                                dcc.Dropdown(
                                    id = 'movie-dropdown',
                                    options = [
                                        {'label': 'Black Panther: Wakanda Forever', 'value': 'black panther wakanda forever'},
                                        {'label': 'Black Panther', 'value': 'black panther'},
                                        {'label': 'Thor: Love and Thunder', 'value': 'thor love and thunder'},
                                        {'label': 'She-Hulk: Attorney at Law', 'value': 'she hulk attorney at law'},
                                        {'label': 'Spider-Man: No Way Home', 'value': 'spider man no way home'},
                                        {'label': 'Avengers: Endgame', 'value': 'avengers endgame'},
                                        {'label': 'Doctor Strange in the Multiverse of Madness', 'value': 'doctor strange in the multiverse of madness'},
                                        {'label': 'Eternals', 'value': 'eternals'},
                                        {'label': 'Avengers: Infinity War', 'value': 'avengers infinity war'},
                                        {'label': 'Iron Man', 'value': 'iron man'},
                                        {'label': 'Guardians of the Galaxy', 'value': 'guardians of the galaxy'},
                                        {'label': 'Moon Knight', 'value': 'moon knight'},
                                        {'label': 'Loki', 'value': 'loki'},
                                        {'label': 'The Avengers', 'value': 'the avengers'},
                                        {'label': 'WandaVision', 'value': 'wandavision'},
                                        {'label': 'Black Widow', 'value': 'black widow'},
                                        {'label': 'Guardians of the Galaxy Vol. 2', 'value': 'guardians of the galaxy vol 2'},
                                        {'label': 'Shang-Chi and the Legend of the Ten Rings', 'value': 'shang chi and the legend of the ten rings'},
                                        {'label': 'Captain America: The First Avenger', 'value': 'captain america the first avenger'},
                                        {'label': 'Thor: Ragnarok', 'value': 'thor ragnarok'},
                                        {'label': 'Captain America: Civil War', 'value': 'captain america civil war'},
                                        {'label': 'Thor', 'value': 'thor'},
                                        {'label': 'Spider-Man 3', 'value': 'spider man 3'},
                                        {'label': 'Captain Marvel', 'value': 'captain marvel'},
                                        {'label': 'Spider-Man: Homecoming', 'value': 'spider man homecoming'},
                                        {'label': 'Avengers: Age of Ultron', 'value': 'avengers age of ultron'},
                                        {'label': 'Hawkeye', 'value': 'hawkeye'},
                                        {'label': 'Doctor Strange', 'value': 'doctor strange'},
                                        {'label': 'Ms. Marvel', 'value': 'ms marvel'},
                                        {'label': 'Iron Man 2', 'value': 'iron man 2'},
                                        {'label': 'What If...?', 'value': 'what if'},
                                        {'label': 'Ant-Man', 'value': 'ant man'},
                                        {'label': 'Spider-Man: Far from Home', 'value': 'spider man far from home'},
                                        {'label': 'Captain America: The Winter Soldier', 'value': 'captain america the winter soldier '},
                                        {'label': 'The Incredible Hulk', 'value': 'the incredible hulk'},
                                        {'label': 'Iron Man 3', 'value': 'iron man 3'},
                                        {'label': 'The Falcon and the Winter Soldier', 'value': 'the falcon and the winter soldier'},
                                    ],
                                    value = 'black panther wakanda forever',
                                    multi = False,
                                    style = {"width": "70%"},
                                    clearable = False
                                ),
                                dcc.Graph(id='movie-graph'),
                            ], className = 'pie-chart'
                        )
                    ], className = 'container-under-left'
                ),
                html.Div(
                    children = [
                        html.H2('Other Movies Recommendation'),
                        html.Div(id='movie-table')
                    ], className = 'container-under-right'
                )
            ], className = 'container-under'
        )
    ], className = 'container'
)

#---------------------------------------------------------------
@app.callback(
    Output(component_id='movie-graph', component_property='figure'),
    Input(component_id='movie-dropdown', component_property='value')
)

def sentiment_analysis_plot(movie):
    df_movie = df[df['movie']==movie]
    sentiment_count = df_movie['sentiment'].value_counts().reset_index().rename(columns={'sentiment':'proportion','index':'sentiment'})
    from_time = df_movie['datetime'].apply(lambda x:str(x).split()[0]).min()
    to_time = df_movie['datetime'].apply(lambda x:str(x).split()[0]).max()
    total_amount = sentiment_count['proportion'].sum()
    piechart = px.pie(
        data_frame = sentiment_count,
        values = 'proportion',
        names = 'sentiment',
        hole = .44,
        title = f'Overview {total_amount} reddit posts between {from_time} and {to_time}'
    )
    piechart.update_layout(
        autosize = False,
        width = 660,
        height = 440,
        title_font_family = 'Space Mono',
        font_family = 'Space Mono'
    )
    
    return piechart

@app.callback(
    Output(component_id='movie-table', component_property='children'),
    Input(component_id='movie-dropdown', component_property='value')
)

def movies_recommendation(movie):
    name_mapping = {}
    primary_name = (pd.Series(recommendation.columns)).sort_values().to_list()
    lower_name = df['movie'].drop_duplicates().sort_values().to_list()
    for i in range(len(primary_name)):
        name_mapping[lower_name[i]] = primary_name[i]

    movies_recomm = recommendation[name_mapping[movie]].sort_values(ascending=False).head(10).reset_index().drop(name_mapping[movie],axis=1)
    movies_recomm.columns = ['']

    return dt.DataTable(data=movies_recomm.to_dict('records'),columns=[{"name": i, "id": i,} for i in (movies_recomm.columns)])

if __name__ == '__main__':
    app.run_server(debug=True)
