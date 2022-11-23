from psaw import PushshiftAPI
import datetime as dt
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import warnings
import re
import sqlalchemy
from sqlalchemy.orm import sessionmaker
import sqlite3
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
# pd.set_option('display.max_colwidth', None)
pd.set_option('display.max_columns', None)
warnings.filterwarnings("ignore")

spark = SparkSession.builder.getOrCreate()

#Extract phase
def extract(term,number_posts,start_time,end_time):
    term = '&'.join(term.split())
    start_time = int(dt.datetime(int(start_time.split('-')[0]),int(start_time.split('-')[1]),int(start_time.split('-')[2])).timestamp())
    end_time = int(dt.datetime(int(end_time.split('-')[0]),int(end_time.split('-')[1]),int(end_time.split('-')[2])).timestamp())
    api = PushshiftAPI()
    data = list(
        api.search_submissions(
            q=term,
            limit=number_posts,
            after=start_time,
            before=end_time,
            author='!AutoModerator',
            filter=['id','author','created_utc','domain','url','title','selftext','num_comments','score']
        )
    )
    #Ensure the data is correct
    data = [i for i in data if len(i)==11]
    #Create spark dataframe
    df = spark.createDataFrame(data)
    if df.count() != 0:
        df = df.withColumn('datetime',from_unixtime(df['created_utc']))
        df = df.drop('d_','created')
        df = df.withColumn('movie',lit(term))
        print(f'Got {df.count()} reddit posts available for {term} from {start_time} to {end_time} utc.')

    else:
        print(f'No reddit post available for {term} from {start_time} to {end_time} utc.')
    
    return df

#Transform phase
def remove_emojis(data):
    emoj = re.compile("["
        u"\U0001F600-\U0001F64F"  
        u"\U0001F300-\U0001F5FF"  
        u"\U0001F680-\U0001F6FF" 
        u"\U0001F1E0-\U0001F1FF"  
        u"\U00002500-\U00002BEF" 
        u"\U00002702-\U000027B0"
        u"\U00002702-\U000027B0"
        u"\U000024C2-\U0001F251"
        u"\U0001f926-\U0001f937"
        u"\U00010000-\U0010ffff"
        u"\u2640-\u2642" 
        u"\u2600-\u2B55"
        u"\u200d"
        u"\u23cf"
        u"\u23e9"
        u"\u231a"
        u"\ufe0f"  
        u"\u3030"
                      "]+", re.UNICODE)
    return re.sub(emoj, '', data)

def transform(df): 
    #Ensure primary key is unique
    df = df.drop_duplicates(subset=['id'])
    #Remove posts that have same content
    df = df.drop_duplicates(subset=['selftext','title'])
    #Remove posts that have empty content
    df = df.dropna(subset=['selftext','title'],how='all')
    #Fill missing values of content
    df = df.fillna(value='',subset=['title','selftext'])
    #Remove \r character
    df = df.withColumn('selftext',regexp_replace('selftext','[\r]+',' '))
    df = df.withColumn('title',regexp_replace('title','[\r]+',' '))
    #Remove \n character
    df = df.withColumn('selftext',regexp_replace('selftext','[\n]+',' '))
    df = df.withColumn('title',regexp_replace('title','[\n]+',' '))
    #Replace removed content with empty string
    df = df.withColumn('selftext',regexp_replace('selftext','\[removed\]',''))
    df = df.withColumn('title',regexp_replace('title','\[removed\]',''))
    df = df.withColumn('selftext',regexp_replace('selftext','\[deleted\]',''))
    df = df.withColumn('title',regexp_replace('title','\[deleted\]',''))
    #Remove &amp;#x200B characters
    df = df.withColumn('selftext',regexp_replace('selftext','&amp;#x200B',' '))
    df = df.withColumn('title',regexp_replace('title','&amp;#x200B',' '))
    #Remove URL 
    df = df.withColumn('selftext',regexp_replace('selftext','http\S+',' '))
    df = df.withColumn('title',regexp_replace('title','http\S+',' '))
    #Replace ! with .
    df = df.withColumn('selftext',regexp_replace('selftext','!','.'))
    df = df.withColumn('title',regexp_replace('title','!','.'))
    #Replace multilple points with single point
    df = df.withColumn('selftext',regexp_replace('selftext','[.]+','. '))
    df = df.withColumn('title',regexp_replace('title','[.]+','. '))
    #Remove u/ character
    df = df.withColumn('selftext',regexp_replace('selftext','u/',' '))
    df = df.withColumn('title',regexp_replace('title','u/',' '))
    #Remove emojis
    remove_emojis_udf = udf(remove_emojis,StringType())
    df = df.withColumn('selftext',remove_emojis_udf(df['selftext']))
    df = df.withColumn('title',remove_emojis_udf(df['title']))
    #Remove special characters
    df = df.withColumn('selftext',regexp_replace('selftext','[-—–_~%&\\\/;:"“”‘’•|,<>?#@àè\^\(\)\*\'\[\]]+',' '))
    df = df.withColumn('title',regexp_replace('title','[-—–_~%&\\\/;:"“”‘’•|,<>?#@àè\^\(\)\*\'\[\]]+',' '))
    #Remove redundant spaces
    df = df.withColumn('selftext',regexp_replace('selftext','[\s]+',' '))
    df = df.withColumn('title',regexp_replace('title','[\s]+',' '))
    #Trim the space of texts
    df = df.withColumn('selftext',trim(df['selftext']))
    df = df.withColumn('title',trim(df['title']))
    #Sort data by datetime
    df = df.orderBy(col('datetime').asc())

    return df

#Load phase
def load(df,list_id):
    DATABASE_LOCATION = 'sqlite:///imdb_movies.sqlite'
    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    conn = sqlite3.connect('imdb_movies.sqlite')
    cursor = conn.cursor()

    sql_query = """
    CREATE TABLE IF NOT EXISTS marvel_movies_reddit(
        author VARCHAR,
        created_utc INT,
        domain VARCHAR,
        id VARCHAR,
        num_comments INT,
        score INT,
        selftext VARCHAR,
        title VARCHAR,
        url VARCHAR,
        datetime DATETIME,
        movie VARCHAR,
        CONSTRAINT primary_key_constraint PRIMARY KEY (id)
    )
    """
    cursor.execute(sql_query)
    print("Open database successfully")
    
    df_ps = df.toPandas()
    df_ps = df_ps[~df_ps['id'].isin(list_id)].reset_index(drop=True)
    df_ps.to_sql('marvel_movies_reddit', engine, index=False, if_exists='append')

    conn.close()
    print("Close database successfully")

    return df_ps

if __name__ == '__main__':
    marvel_movies = {
        'black panther wakanda forever': '2022-11-11',
        'black panther': '2018-02-16',
        'thor love and thunder': '2022-07-08',
        'she hulk attorney at law': '2022-08-18',
        'spider man no way home': '2021-12-17',
        'avengers endgame': '2019-04-26',
        'doctor strange in the multiverse of madness': '2022-05-06',
        'eternals': '2021-11-05',
        'avengers infinity war': '2018-04-27',
        'iron man': '2008-05-02',
        'guardians of the galaxy': '2014-08-01',
        'moon knight': '2022-03-30',
        'loki': '2021-06-09',
        'the avengers': '2012-05-04',
        'wandavision': '2021-01-15',
        'black widow': '2021-07-09',
        'guardians of the galaxy vol 2': '2017-05-05',
        'shang chi and the legend of the ten rings': '2021-09-03',
        'captain america the first avenger': '2011-07-22',
        'thor ragnarok': '2017-11-03',
        'captain america civil war': '2016-05-06',
        'thor': '2011-05-06',
        'spider man 3': '2007-05-04',
        'captain marvel': '2019-03-08',
        'spider man homecoming': '2017-07-07',
        'avengers age of ultron': '2015-05-01',
        'hawkeye': '2021-11-24',
        'doctor strange': '2016-11-04',
        'ms marvel': '2022-06-08',
        'iron man 2': '2010-05-07',
        'what if': '2021-08-11',
        'ant man': '2015-07-17',
        'spider man far from home': '2019-07-02',
        'captain america the winter soldier': '2014-04-04',
        'the incredible hulk': '2008-06-13',
        'iron man 3': '2013-05-03',
        'the falcon and the winter soldier': '2021-03-19'
    }
    list_id = []
    for movie in marvel_movies.keys():
        print(f'Start to collect {movie}')
        from_date = marvel_movies[movie]
        to_date = str(int(from_date[:4])+1) + from_date[4:]
        try:
            print('Begin extract phase.')
            df = extract(term=movie,number_posts=10000,start_time=from_date,end_time=to_date)
            print('End extract phase.')
            print('Begin transform phase.')
            df = transform(df)
            print(f'Got {df.count()} reddit posts available after transform phase.')
            print('End transform phase.')
            print('Begin load phase.')
            df_ps = load(df,list_id)
            list_id += list(df_ps['id'].unique())
            print(f'Got {len(df_ps)} reddit posts available after load phase.')
            print('End load phase.')
            print('All process completed.')
            print(f'Total reddit posts: {len(list_id)}')
            print('**************************************************************************')
        except:
            print(f'Cannot get movie {movie} post from reddit')
            print('**************************************************************************')


