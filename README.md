
# Automated Data Pipeline For Sentiment Analysis and Recommendation System About Marvel Cinematic Universe Movies

The main goal of this project is to build and maintain an ETL pipeline that stores the popular Marvel movie data and Reddit posts that mention them (the pipeline can capture the data in real time), then analyze the sentiment of people toward these movies based on the collected data. We also leverage the available dataset from the top 5000 movies on the IMDB website to build a recommendation system to suggest movies that are similar to these Marvel movies. In the end, we will build a dashboard that visualizes the proportion of the audience’s sentiment and the other recommended movies for each Marvel movie.


![MarvelStudios](https://raw.githubusercontent.com/dribblewithclong/Automated-Data-Pipeline-For-Sentiment-Analysis-About-Marvel-Cinematic-Universe-Movies/main/assets/marvel.jpg)


## Authors

- [@dribblewithclong](https://www.github.com/dribblewithclong)


## Web App Preview

![App Screenshot](https://via.placeholder.com/468x300?text=App+Screenshot+Here)


## Tools Used
- [Python](https://docs.python.org/3/): A programming language for writing scripts.
    - [Selenium](https://selenium-python.readthedocs.io/): Used for data scraping.
    - [Pyspark](https://spark.apache.org/docs/latest/api/python/), [Pandas](https://pandas.pydata.org/docs/), [Numpy](https://numpy.org/doc/1.23/): Used for data wrangling.
    - [VADER](https://github.com/cjhutto/vaderSentiment): Used for sentiment analysis.
    - [Gensim](https://radimrehurek.com/gensim/auto_examples/index.html#documentation): Used for text processing with NLP models.
    - [Scikit-learn](https://scikit-learn.org/stable/): Used for recommendation system.
- [SQLite](https://www.sqlite.org/docs.html): A relational database management system for storing data.
- [Dash Plotly](https://dash.plotly.com/): A platform for writing and creating interactive web applications.
- [Apache Airflow](https://airflow.apache.org/docs/): An open-source platform for developing, scheduling, and monitoring batch-oriented workflows.
- [Heroku](https://devcenter.heroku.com/categories/reference): A platform as a service (PaaS) that enables developers to build, run, and operate applications entirely in the cloud.

## Data Sources
The two main data sources that we used for this project are collected from [Reddit](https://www.reddit.com) and [IMDB](https://www.imdb.com/) websites. The [IMDB movie data](https://www.imdb.com/interfaces/) is public on the internet while the Reddit data is scraped through API.
## Workflow

### 1. [Popular Marvel movie information scraping](https://github.com/dribblewithclong/Automated-Data-Pipeline-For-Sentiment-Analysis-About-Marvel-Cinematic-Universe-Movies/blob/main/marvel_movies_info_crawl.py)
- The purpose of this step is to collect the top popular movies of Marvel Studios' information from the IMDB website. The data collected includes name, duration, genre, release date, average rating, number ratings, overview, and tagline of the movies.
- To achieve this, I used Selenium to scrape these information. First, I navigated to the link which has a list of popular movies from Marvel Studios, then I created a function to collect all the links to each individual movie and store them in a list. After having these links, I created a function to collect all the features of the movie that I listed above. The last step is to apply this function for all the links of the movie through the loop and load the data to the database when it is collected ultimately.

### 2. Data pipeline of user submission on Reddit
- After having the information about the top popular movies of Marvel Studios, I built a data pipeline that uses the names of films collected in the above step as input. The purpose of this data pipeline is to auto-collect posts in Reddit where people discuss the movies, then process and transform this raw data into a suitable format, and finally load this processed data to the databases from where I can query the dataset to analyze the sentiment of people to these movies.

#### 2.1.  [ETL process](https://github.com/dribblewithclong/Automated-Data-Pipeline-For-Sentiment-Analysis-About-Marvel-Cinematic-Universe-Movies/blob/main/reddit_etl.py)
- In this process, I used Pyspark to manipulate the collected data and SQLite database to store it.
- In the Extract phase, I used an API library of Reddit called Pushshift API to extract the Reddit posts from users about movies. The function of it has inputs such as the search term (name of the movie in this case), the max number of posts that I want to get, the start and end time, and which field of data I want to collect (I will get the id, author, time created, the domain, link, title, content, number comments and the score of each post in this case).
- In the Transform phase, first, I need to ensure the primary key (id of the post in this case) is unique i.e. not allow the duplicated data to happen before it is loaded to the database, then I used some methods to preprocess and transform the data collected from the Extract phase. They include removing the posts that have the same or empty content, removing redundant space, removing URLs, special messy characters, and emoji of content.
- In the Load phase, first, I chose the location of the database, then I connected to the database and created a table using the SQL syntax, and finally, loaded the transformed data to it.

#### 2.2. [Sentiment analysis](https://github.com/dribblewithclong/Automated-Data-Pipeline-For-Sentiment-Analysis-About-Marvel-Cinematic-Universe-Movies/blob/main/reddit_sentiment_analysis.ipynb)

#### 2.3. [Scheduled real-time automated data pipeline](https://github.com/dribblewithclong/Automated-Data-Pipeline-For-Sentiment-Analysis-About-Marvel-Cinematic-Universe-Movies/blob/main/dags/reddit_dag.py)

### 3. Movie recommendation system

#### 3.1. [Data preprocessing](https://github.com/dribblewithclong/Automated-Data-Pipeline-For-Sentiment-Analysis-About-Marvel-Cinematic-Universe-Movies/blob/main/imdb_movies_cleaning.ipynb)

#### 3.2. [Recommendation system building](https://github.com/dribblewithclong/Automated-Data-Pipeline-For-Sentiment-Analysis-About-Marvel-Cinematic-Universe-Movies/blob/main/movies_recommendation.ipynb)

### 4. [Dashboard visualization and deployment](https://github.com/dribblewithclong/Automated-Data-Pipeline-For-Sentiment-Analysis-About-Marvel-Cinematic-Universe-Movies/blob/main/app.py)
