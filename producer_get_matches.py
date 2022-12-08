import requests
from datetime import date, timedelta
from bs4 import BeautifulSoup

import datetime
import json
import time
from selenium import webdriver 
from selenium.webdriver.chrome.options import Options
from kafka import KafkaProducer

Options = Options()
Options.headless = True 
webdriver = webdriver.Chrome()

def get_matches_as_json():
    today = date.today()
    tomorrow = date.today() + timedelta(1)

    scores_web_site =  f"https://theathletic.com/nba/schedule/{today}/" 
    upcoming_matches_site = f"https://theathletic.com/nba/schedule/{tomorrow}/" 

    webdriver.get(scores_web_site )
    time.sleep(1)

    webdriver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(1)
    html = BeautifulSoup(webdriver.page_source,'lxml')

    teams = [
        item.get_text(strip=True) for item in html.find_all(class_="jss15")
    ]

    scores =  [
        item.get_text(strip=True) for item in html.find_all(class_="sc-c8ee3952-0 sc-9273112-16 cOGVhH ciCLxm")
    ]

    scores_dict = {}
    scores_list = []
    for i in range(round(len(teams)/2)):
        scores_dict['home'] = teams[0]
        scores_dict['away'] = teams[1]
        scores_dict['score'] = scores[i]    
        scores_list.append(scores_dict.copy())
        del teams[0:2]

    # Get Upcoming Matches
    webdriver.get(upcoming_matches_site)
    time.sleep(1)

    webdriver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(1)
    html = BeautifulSoup(webdriver.page_source,'lxml')

    teams = [
        item.get_text(strip=True) for item in html.find_all(class_="jss15")
    ]

    times =  [
        item.get_text(strip=True) for item in html.find_all(class_="sc-c8ee3952-0 sc-9273112-16 cOGVhH ciCLxm")
    ]


    upcoming_dict = {}
    upcoming_list = []
    for i in range(round(len(teams)/2)):
        upcoming_dict['home'] = teams[0]
        upcoming_dict['away'] = teams[1]
        upcoming_dict['time'] = times[i]    
        upcoming_list.append(upcoming_dict.copy())
        del teams[0:2]


    json_dict = {"scores": scores_list, "upcoming_mathces": upcoming_list}
    return json.dumps(json_dict)


MATCH_KAFKA_TOPIC = 'match_details'
producer = KafkaProducer(bootstrap_servers="localhost:29092",
                         api_version=(0, 10, 1))

print("Going to be get match details data!")

data = get_matches_as_json()
# print(data)
producer.send(MATCH_KAFKA_TOPIC, data.encode("utf-8"))
producer.flush()
print("Done Sending......")
