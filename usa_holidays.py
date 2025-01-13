import json
import requests
import mysql.connector
import pytz
from datetime import datetime

tz = pytz.timezone('America/New_York')
fmp_api_key = "fmp_api_key_here"
polygon_api_key = "polygon_api_key_here"


def connect_mariaDB():
    dbConfig = {
                    'host': '127.0.0.1',
                    'user': 'user',
                    'password': '',
                    'database': 'usa_test' }
    try:
        connection = mysql.connector.connect(**dbConfig,autocommit = True,buffered = True)
    except mysql.connector.Error as err:
        print(f"MariaDB Connection Error:{err}")
    return connection


def getHolidays(source = 'polygon'):
    try:

        data = None
        if source == 'fmp':
            data = []

            exchanges = ['NASDAQ','NYSE','AMEX']

            for exchange in exchanges:
                url = f"https://financialmodelingprep.com/api/v3/is-the-market-open?exchange={exchange}&apikey={fmp_api_key}"
                response = requests.get(url)
                jsonData = response.json()

                for page in jsonData['stockMarketHolidays']:
                    for key,value in page.items():
                        if key == 'year':
                            continue
                        data.append({'date' : value,'exchange' : exchange,'name' : key})

        else:
            url = "https://api.polygon.io/v1/marketstatus/upcoming?apiKey={polygon_api_key}"
            response = requests.get(url)
            data = response.json()

            for i in data:
                if 'close' in i:
                    i['close'] = i['close'].replace("T"," ")
                    i['close'] = i['close'].replace(".000Z","")
                    i['open'] = i['open'].replace("T"," ")
                    i['open'] = i['open'].replace(".000Z","")

                    i['close'] = (datetime.strptime(i['close'],"%Y-%m-%d %H:%M:%S").astimezone(tz)).strftime("%Y-%m-%d %H:%M:%S")
                    i['open'] = (datetime.strptime(i['open'],"%Y-%m-%d %H:%M:%S").astimezone(tz)).strftime("%Y-%m-%d %H:%M:%S")

    except Exception as e:
        print("Get Holidays error :",e)

    return data

def createHolidayTable():
    try:
        connection = connect_mariaDB()
        query = "create table if not exists usa_daily.holidays (date date , exchange varchar(10) ,description varchar(50),market_open datetime,market_close datetime,primary key (date,exchange));"
        cursor = connection.cursor()
        cursor.execute(query)
    except Exception as e:
        print("Create Holiday Table Error :",e)


def insertHolidays(insertString):
    try:
        connection = connect_mariaDB()
        query = f"insert into usa_daily.holidays (date,exchange,description,market_open,market_close) values {insertString} on duplicate key update description = values(description),market_open = values(market_open),market_close = values(market_close);"
        print()
        #print(query)
        cursor = connection.cursor()
        cursor.execute(query)
        print("\nData Inserted.")
    except Exception as e:
        print("Insert Holiday Table Error :",e)


def main():

    createHolidayTable()
    #data = getHolidays('fmp')
    data = getHolidays()


    insertString = ""
    for i in data:
        name = i['name']
        name = name.replace("'","")

        if 'close' in i:
            tempString = f"('{i['date']}','{i['exchange']}','{name}','{i['open']}','{i['close']}'),"
        else:
            tempString = f"('{i['date']}','{i['exchange']}','{name}',NULL,NULL),"
        print(tempString)
        insertString += tempString
    insertString = insertString[:-1]
    insertHolidays(insertString)

main()
