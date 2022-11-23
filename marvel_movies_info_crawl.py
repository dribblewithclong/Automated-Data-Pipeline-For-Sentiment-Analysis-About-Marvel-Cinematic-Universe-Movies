from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver import FirefoxOptions
import numpy as np
import pandas as pd

def get_url_movie_info_from_marvel():    
    opts = FirefoxOptions()
    opts.add_argument("--headless")
    driver = webdriver.Firefox(options=opts)
    driver.maximize_window() #For maximizing window
    driver.implicitly_wait(10) #Gives an implicit wait for 10 seconds
    url = 'https://www.imdb.com/search/title/?companies=co0051941'
    driver.get(url)
    total_height = int(driver.execute_script('return document.body.scrollHeight'))
    for i in range(1, total_height, 100):
        driver.execute_script('window.scrollTo(0, {});'.format(i))
    #Get url of movies
    urls_element = driver.find_elements(by=By.CSS_SELECTOR,value='[class="lister-item-header"]')
    urls = [url_element.find_element(by=By.CSS_SELECTOR,value='a').get_attribute('href') for url_element in urls_element]

    return urls 
    
def get_movie_infomation(url):
    opts = FirefoxOptions()
    opts.add_argument("--headless")
    driver = webdriver.Firefox(options=opts)
    driver.maximize_window() #For maximizing window
    driver.implicitly_wait(10) #Gives an implicit wait for 10 seconds
    driver.get(url)
    total_height = int(driver.execute_script('return document.body.scrollHeight'))
    for i in range(1, total_height, 100):
        driver.execute_script('window.scrollTo(0, {});'.format(i))
    #Get name of the movies
    try:
        names = driver.find_elements(by=By.CSS_SELECTOR,value='[class="sc-80d4314-1 fbQftq"]')[0].text.split('\n')[0]
    except:
        names = 'None'
    #Get the duration of the movies
    try:
        durations = driver.find_elements(by=By.CSS_SELECTOR,value='[class="sc-80d4314-1 fbQftq"]')[0].text.split('\n')[-1]
    except:
        durations = 'None'
    #Get genre of the movies
    try:
        genres = driver.find_elements(by=By.CSS_SELECTOR,value='[class="ipc-chip-list__scroller"]')[0].text.split('\n')
        genres = ','.join(genres)
    except:
        genres = 'None'
    #Get overview of the movies
    try:
        overview = driver.find_elements(by=By.CSS_SELECTOR,value='[class="sc-16ede01-6 cXGXRR"]')[0].text
    except:
        overview = 'None'
    #Get release date of the movies
    try:
        release_dates = driver.find_elements(by=By.CSS_SELECTOR,value='[data-testid="title-details-releasedate"]')[0].text.split('\n')[-1].split(' (')[0]
    except:
        release_dates = None
    #Get tag line of the movies
    try:
        tags = driver.find_elements(by=By.CSS_SELECTOR,value='[data-testid="storyline-taglines"]')[0].text.split('\n')[-1]
    except:
        tags = 'None'
    #Get average rating of the movies
    try:
        ratings = driver.find_elements(by=By.CSS_SELECTOR,value='[data-testid="hero-rating-bar__aggregate-rating__score"]')[0].text.split('\n')[0]
        ratings = float(ratings)
    except:
        ratings = np.nan
    #Get number ratings of the movies
    try:
        num_ratings = driver.find_elements(by=By.CSS_SELECTOR,value='[class="sc-7ab21ed2-3 dPVcnq"]')[0].text
    except:
        num_ratings = np.nan
        
    return [names,durations,genres,overview,release_dates,tags,ratings,num_ratings]

def get_completed_movie_information():
    movie_information = []
    movie_urls = get_url_movie_info_from_marvel()
    for movie_url in movie_urls:
        movie_information.append(get_movie_infomation(movie_url))
        print(f'Got {movie_url}')
        movie_data = pd.DataFrame(movie_information,columns=['primaryTitle','runtimeMinutes','genres','overview','release_date','tagline','averageRating','numVotes'])
        # movie_data.to_csv('imdb_marvel.csv',sep='|',index=False)
        
    return movie_data

