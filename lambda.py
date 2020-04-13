from bs4 import BeautifulSoup
import urllib.request
import re
import unidecode
import json
import pandas as pd

cities = [
    "Phoenix",
    "Chicago",
    "Los-Angeles",
    "New-York",
    "San-Francisco",
    "Washington",
    "Seattle",
    "Austin",
    "Boston",
    "Denver",
    "Philadelphia"]

keys = [
    "One-way Ticket (Local Transport) ",
    "Monthly Pass (Regular Price) ",
    "Taxi Start (Normal Tariff) ",
    "Taxi 1 mile (Normal Tariff) ",
    "Taxi 1hour Waiting (Normal Tariff) ",
    "Gasoline (1 gallon) ",
    "Basic (Electricity, Heating, Cooling, Water, Garbage) for 915 sq ft Apartment ",
    "1 min. of Prepaid Mobile Tariff Local (No Discounts or Plans) ",
    "Internet (60 Mbps or More, Unlimited Data, Cable/ADSL) "
    
]


def get_latest_data(cities, keys):
    """
    This will return dict of dict.
    The first dict will have cities as keys and for every city it will have key value for every key in the keys list.
    
    """
    
    url_prefix = "https://www.numbeo.com/cost-of-living/in/"
    data_dict = {}
    for citi in cities:
        temp_dict = {}
        url = url_prefix+citi
        page = urllib.request.urlopen(url)
        soup = BeautifulSoup(page)
        soup.unicode
        rows = soup.find_all("tr")
        for row in rows:
            cells = row.find_all("td")
            for c in cells:
                #print(c.get_text())
                if c.get_text() in keys:
                    #print(c.get_text())
                    #print(cells[1].get_text())
                    temp_dict[c.get_text()] = unidecode.unidecode(cells[1].get_text())
                
        data_dict[citi] = temp_dict
    
    return data_dict

def convertDictToCsv(data_dict):
    csvdict={}
    csvdict['Cities']={}
    Col_Name=[]
    for col in list(data_dict[cities[0]].keys()):
        csvdict[col]={}
        i=0
        for city in data_dict:
            csvdict[col][str(i)]=data_dict[city][col]
            i=i+1
    i=0
    for city in data_dict:
        csvdict['Cities'][str(i)]=city
        i=i+1
    csvdict=json.dumps(csvdict)
    df = pd.read_json (csvdict)
    export_csv = df.to_csv (r'Test.csv', index = None, header=True)
      

    print(csvdict)


convertDictToCsv(get_latest_data(cities, keys))