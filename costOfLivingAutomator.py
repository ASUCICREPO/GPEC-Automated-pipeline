from bs4 import BeautifulSoup
import urllib.request
import re
import unidecode
import json
import pandas as pd
import boto3
from botocore.exceptions import NoCredentialsError
from io import StringIO

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

bucket_name= 'gpec-data'
key_name='AutomatedCities.csv'


def get_latest_data():
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
        soup = BeautifulSoup(page,features="html.parser")
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

def convertDictToCsvAndUpload(data_dict):
    #Converting Dict to CSV
    csvdict={}
    csvdict['Cities']={}
    Col_Name=[]
    for col in list(data_dict[cities[0]].keys()):
        csvdict[col]={}
        i=0
        for city in data_dict:
            # print(data_dict[city][col])
            # print(round(float(data_dict[city][col].replace('$','').strip()),2))
            csvdict[col][str(i)]=round(float(data_dict[city][col].replace('$','')),2)
            i=i+1
    i=0
    for city in data_dict:
        csvdict['Cities'][str(i)]=city
        i=i+1
    csvdict=json.dumps(csvdict)
    df = pd.read_json (csvdict)
    csv_buffer = StringIO()
    df.to_csv(csv_buffer,index = None, header=True)
    s3_resource = boto3.resource('s3')
    s3_resource.Object(bucket_name, key_name).put(Body=csv_buffer.getvalue())
    print('Uploaded')

def handler(event, context):
    convertDictToCsvAndUpload(get_latest_data())
    return {
        'statusCode': 200,
        'body': json.dumps('Latest data pushed!')
    }

# convertDictToCsvAndUpload(get_latest_data())