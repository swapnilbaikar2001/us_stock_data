import time
import mysql.connector
from mysql.connector import pooling
from datetime import datetime,timedelta
from sys import argv
import pytz
import requests
from time import sleep
import concurrent.futures
import threading
import copy

tz = pytz.timezone('America/New_York')
#tz = pytz.timezone('Asia/Kolkata')
isHist = True
isIntraday = True
isDaily = False
tickerDict = {}

isDst = False

#paper trading credentials - swapnil
apiKey = "api_key_here"
apiSecret = "api_secret_here"

headers = {'APCA-API-KEY-ID' : apiKey,'APCA-API-SECRET-KEY' : apiSecret,'accept': 'application/json'}

min_hash = {}
five_min_hash = {}

#insert data at the end,if running for history
bulk_min_hash = {}
bulk_five_min_hash = {}
update_symbol_list = 0
nextMarketStartDate = None
nextEodDate = None

day_hash = {}
dbConfig = {
                    'host': '127.0.0.1',
                    'user': 'user',
                    'password': '',
                    'database': 'usa_test' }

#initialize connection
def connect_mariaDB():
    global dbConfig
    try:
        connection = mysql.connector.connect(**dbConfig,autocommit = True,buffered = True)
    except mysql.connector.Error as err:
        print(f"MariaDB Connection Error:{err}")
    return connection




#check if market open or not
def isMarketOpen(startDate):
    try:
        marketOpen =  True
        url = f"https://paper-api.alpaca.markets/v2/calendar?start={startDate}&end={startDate}&date_type=TRADING"
        response = requests.get(url,headers = headers)
        data = response.json()
        if len(data) != 0:
            marketOpen = True
            data = data[0]

            #check if special session
            if data['date'] == startDate and data['close'] != "16:00":
                marketOpen = data['date'] + " " + data['open'] + ":00"  +"," + data['date'] + " " + data['close'] + ":00"

        else:
            marketOpen = False
    except Exception as e:
        print("Check Market Status Error :")
    finally:
        return marketOpen


#check holiday or not or special session too
def isHoliday(startDate):
    try:
        description = ""
        market_open = ""
        market_close = ""
        holiday = False

        connection = connect_mariaDB()
        cursor = connection.cursor()
        query = f"select description,market_open,market_close from usa_test.holidays where date = '{startDate}' and exchange = 'NASDAQ';"
        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        connection.close()

        rows = []
        if len(rows) > 0:
            for row in rows:
                description = row[0]
                market_open = row[1]
                market_close = row[2]

            #if special session
            if market_open != None and market_close != None:
                holiday = str(market_open) + "," + str(market_close)

            else:
                print(f"{startDate}: {description}")
                holiday = True
        else:
            check_date = (datetime.strptime(startDate,"%Y-%m-%d")).strftime("%A")

            if isMarketOpen(startDate) == True:
                holiday = False

            elif isMarketOpen(startDate) == False:
                if check_date == "Saturday":
                    print(f"{startDate} is Saturday.")
                elif check_date == "Sunday":
                    print(f"{startDate} is Sunday.")

                holiday = True

            else:
                #special session
                holiday = isMarketOpen(startDate)

    except Exception as e:
        print("Check Holiday Error :",e)

    finally:
        return holiday

#check if date is in daylight saving time
#Note : Necessary step as "GMT to New york time conversion" differs by one hour in DST days.
def check_dst(date):
    isDst = False
    tz = pytz.timezone('America/New_York')
    cur_date = datetime.strptime(date,"%Y-%m-%d")
    localized_dt = tz.localize(cur_date)
    if localized_dt.dst() != timedelta(0):
        isDst = True

    return isDst

#parse argument sd,ed,symbol_list,intraday or eod
def parseArgs():

    global isIntraday,isDaily,update_symbol_list
    startDate = str(datetime.now(tz).date())
    endDate = startDate
    isIntraday = True
    update_symbol_list = 0
    if len(argv) > 1:
        argv.pop(0)
        while argv :
            parameter  = argv.pop(0)
            if parameter == "-sd" :
                startDate = argv.pop(0)
            elif parameter == "-ed" :
                endDate = argv.pop(0)
            elif parameter == "-intraday":
                isIntraday = True
                isDaily = False
            elif parameter == "-eod":
                isDaily = True
                isIntraday = False
            elif parameter == '-symbol_list':
                update_symbol_list = 1


    return startDate,endDate

#check if data is jsonable or not
def is_jsonable(x):
    try:
        json.dump(x)
        return True
    except:
        return False


#get all us equity tickers if exchange is not specified explicitly.
def getTickers(exchange = None):
    try:
        global tickerDict

        tickerDict = {}
        tickers = []
        tickers2 = []
        if exchange:
            exchanges = [exchange]
        else:
            exchanges = ['NASDAQ','NYSE','AMEX','ARCA']

        for exchange in exchanges:
            print(f"\nFetching {exchange} tickers..")
            url = f"https://paper-api.alpaca.markets/v2/assets?status=active&asset_class=us_equity&exchange={exchange}&attributes="
            #url = "https://broker-api.alpaca.markets/v1/assets?status=active&asset_class=us_equity&attributes="
            response = requests.get(url, headers=headers)
            tickerData = response.json()

            t = 0
            for i in tickerData:
                if i['tradable']:
                    ticker = i['symbol']
                    if ticker not in tickerDict:
                        tickerDict[ticker] = {}

                        i['name'] = i['name'].replace("'","")
                        i['name'] = i['name'].replace("�"," ")

                        tickerDict[ticker] = {'name' : i['name'], 'exchange' : i['exchange']}
                    t += 1
                    if exchange in ['NASDAQ','AMEX']:
                        tickers.append(ticker)
                    else:
                        tickers2.append(ticker)
            print(f"Tickers in {exchange} :",t)
        print(f"\nFetched {len(tickerDict)} tickers sucessfully.")
    except Exception as e:
        print("Get Tickers error :",e)

    return list(set(tickers)),list(set(tickers2))


#create history tables for ticker if specified as argument,else create tables for all tickers
def createTable(cursor = None,ticker = None):

    global tickerDict

    try:

        #this function is also called from threaded function insertHistoryData...
        #use local cursor from that function.else if not called from Threaded function
        #created own cursor and close it at end.
        createdFlag = False

        if not cursor:

            connection = connect_mariaDB()
            cursor = connection.cursor()
            createdFlag = True

        #create table for single ticker passed as argument,if not create table for all tickers
        if ticker:
            tickers = [ticker]
        else:
            tickers = list(set(tickerDict.keys()))

        a = 1
        for ticker in tickers:
            print(f"\r{a} : {ticker}",end = "")
            a += 1

            exchange = tickerDict[ticker]['exchange']
            exchCode = "nd" if exchange == "NASDAQ" else ("ny" if exchange == "NYSE" else ("mx" if exchange == "AMEX" else "ar"))
            for timeframe in ["1min","5min","daily"]:

                db = "usa_" + timeframe
                #db = "usa_1min" if timeframe == "1min" else ("usa_5min" if timeframe == "5min" else "usa")
                tableName = ticker.lower() + '_' + exchCode + '_' + timeframe

                if timeframe == 'daily':
                    createQuery = f"create table if not exists {db}.`{tableName}` (date date PRIMARY KEY,open float(8,2),high float(8,2),low float(8,2),close float(8,2),volume bigint(20),adj_vol bigint(20),adj_open float(8,2),adj_high float(8,2),adj_low float(8,2),adj_close float(8,2))"
                else:

                    createQuery = f"create table if not exists {db}.`{tableName}` (date datetime PRIMARY KEY,open float(8,2),high float(8,2),low float(8,2),close float(8,2),volume bigint(20),adj_vol bigint(20),adj_open float(8,2),adj_high float(8,2),adj_low float(8,2),adj_close float(8,2))"
                cursor.execute(createQuery)
        print()


    except Exception as e:
        print("Create table Error :",ticker,":",e)

    finally:

        #close cursor and connection if created explicitly.
        if createdFlag:
            cursor.close()
            connection.close()

#fetch EOD data for all tickers
def get_EOD_Data(stringTickers,stringTickers2,startDate):
    try:
        next_page_token = None
        eod_data = []
        for string_ticker in [stringTickers,stringTickers2]:
            next_page = 1
            while next_page == 1:
                url = "https://data.alpaca.markets/v2/stocks/bars"
                params = {"symbols" : string_ticker,"timeframe" : '1D','start' : startDate,'end' : startDate,'limit' : 10000,'adjustment' : 'all','asof' : startDate , 'page_token' : next_page_token}
                response = requests.get(url, headers=headers,params = params)
                data = response.json()
                if "next_page_token" in data:
                    if data["next_page_token"] is not None:
                        next_page_token = data["next_page_token"]
                    else:
                        next_page = 0
                if len(data['bars']) > 0:
                    eod_data.append(data['bars'])
    except Exception as e:
        print("Get Data Error :",response.content,":",e,"\n")

    return eod_data

#fetch intraday data for all tickers
def getIntradayData(stringTickers,stringTickers2,startTick,endTick):

    try:
        next_page_token = None
        intradayData = []

        #convert "2024-11-27 09:31:00" to "2024-11-27T09:31:00Z" to pass in API
        startTick = startTick.replace(" ","T")
        startTick += "Z"
        endTick = endTick.replace(" ","T")
        endTick +="Z"

        for string_ticker in [stringTickers,stringTickers2]:
            next_page = 1

            #loop until next page is available
            while next_page == 1:
                url = "https://data.alpaca.markets/v2/stocks/bars"
                date = startTick.split("T")[0]
                params = {"symbols" : string_ticker,"timeframe" : '1T','start' : startTick,'end' : endTick,'limit' : 10000,'adjustment' : 'all','asof' : date , 'page_token' : next_page_token}
                response = requests.get(url, headers=headers,params = params)
                data = response.json()
                if "next_page_token" in data:
                    if data["next_page_token"] is not None:
                        next_page_token = data["next_page_token"]
                    else:
                        #break when next page is not available
                        next_page = 0

                if len(data['bars']) > 0:
                    intradayData.append(data['bars'])

    except Exception as e:
        print("Get Data Error :",response.content,":",e,"\n")

    return intradayData

#generate 5 min OHLCV-OI data using min_hash(1 min data)
def generateFiveMinOHLC(ticker):
    global five_min_hash,min_hash

    if ticker in list(set(min_hash.keys())):

        dateTime = list(min_hash[ticker].keys())[0]
        fiveMinTick = getNextFiveMinTick(dateTime)
        min_open = float(min_hash[ticker][dateTime]['open'])
        min_high = float(min_hash[ticker][dateTime]['high'])
        min_low = float(min_hash[ticker][dateTime]['low'])
        min_close = float(min_hash[ticker][dateTime]['close'])
        min_volume = int(min_hash[ticker][dateTime]['volume'])

        if ticker in five_min_hash:

            five_min_high = float(five_min_hash[ticker][fiveMinTick]['high'])
            five_min_hash[ticker][fiveMinTick]['high'] = max(min_high,five_min_high)

            five_min_low = float(five_min_hash[ticker][fiveMinTick]['low'])
            five_min_hash[ticker][fiveMinTick]['low'] = min(min_low,five_min_low)

            five_min_hash[ticker][fiveMinTick]['close'] = float(min_close)

            five_min_hash[ticker][fiveMinTick]['volume']+= int(min_volume)


        #if first tick of minute,set OHLC as min_hash
        else:
            five_min_hash[ticker] = {}
            five_min_hash[ticker][fiveMinTick] = {'open' : min_open,'high' : min_high ,'low' : min_low,'close' : min_close,'volume' : min_volume}

#eg: if we input "2024-08-20 09:17:00" it gives "2024-08-20 09:20:00"
def getNextFiveMinTick(timestamp):

    #ts in timestamp in unix Epoch Time
    ts = int(datetime.strptime(timestamp,"%Y-%m-%d %H:%M:%S").timestamp())

    #if 09:20:00 return 09:20:00
    if ts % 300 == 0:
        nextTick = ts
    else:

        #nextTick is unix Epoch Timestamp of next five min Tick
        #if 09:21:00 return 09:25:00
        nextTick = ((ts // 300) + 1) * 300

    #convert unix timestamp to "2024-08-20 09:20:00" format
    return (datetime.utcfromtimestamp(nextTick).strftime("%Y-%m-%d %H:%M:%S"))

#create stock_list_1min table
def createStockListTable():
    try:

        connection = connect_mariaDB()
        cursor = connection.cursor()

        for db in ['usa_daily','usa_1min']:
            if db == 'usa_1min':
                table = "stock_list_1min"
                query = f"create table if not exists {db}.{table} (ticker_ex varchar(20) PRIMARY KEY,name mediumtext,exchange varchar(20),cur_date datetime,cur_open float(10,2),cur_high float(10,2),cur_low float(10,2),cur_close float(10,2),cur_vol bigint(20),5min_cur_open float(10,2),5min_cur_high float(10,2),5min_cur_low float(10,2),5min_cur_close float(10,2),5min_cur_vol bigint(20),5min_date datetime,5min_open float(10,2),5min_high float(10,2),5min_low float(10,2),5min_close float(10,2),5min_vol bigint(20));"
            else:
                table = "stock_list"
                query = f"create table if not exists {db}.{table} (`ticker_ex` varchar(20) PRIMARY KEY,`name` mediumtext,`exchange` varchar(20),`cur_date` date,`cur_open` float(10,2),`cur_high` float(10,2),`cur_low` float(10,2),`cur_close` float(10,2),`cur_vol` bigint(20),`prev_close` float(8,2),`cur_open(1)` float(10,2),`cur_high(1)` float(10,2),`cur_low(1)` float(10,2),`cur_close(1)` float(10,2),`cur_vol(1)` bigint(20));"
            cursor.execute(query)
        cursor.close()
        connection.close()
    except Exception as e:
        print("create StockList Error :",query,e)

def insertTickerData():
    try:

        connection = connect_mariaDB()
        tickerDataString = ""

        i = 1
        for ticker in set(tickerDict.keys()):
            tempString = f"('{ticker}','{tickerDict[ticker]['name']}','{tickerDict[ticker]['exchange']}'),"
            #print(i,":",tempString)
            i += 1

            tickerDataString += tempString

        tickerDataString = tickerDataString[:-1]

        if tickerDataString != "":
            cursor = connection.cursor()

            for db in ['usa_1min','usa_daily']:
                table = "stock_list_1min" if db == 'usa_1min' else "stock_list"
                query = f"insert into {db}.{table} (ticker_ex,name,exchange) values {tickerDataString} on duplicate key update name = values(name),exchange = values(exchange);"
                cursor.execute(query)
        cursor.close()
    except Exception as e:
        print("insert Ticker Data Error :",e)

    finally:
        if connection:
            connection.close()

#insert data in stock_list_1min table(intraday)
def insertStockListMin():
    try:

        connection = connect_mariaDB()

        cursor = connection.cursor()
        tickers = min_hash.keys()

        insertString = ""
        for ticker in tickers:

            cur_date = list(min_hash[ticker].keys())[0]
            dateTime = cur_date
            cur_open =  "{:.2f}".format(float(min_hash[ticker][dateTime]['open']))
            cur_high =  "{:.2f}".format(float(min_hash[ticker][dateTime]['high']))
            cur_low =   "{:.2f}".format(float(min_hash[ticker][dateTime]['low']))
            cur_close = "{:.2f}".format(float(min_hash[ticker][dateTime]['close']))
            cur_vol = min_hash[ticker][dateTime]['volume']

            fiveMinTime = list(five_min_hash[ticker].keys())[0]
            five_min_cur_open = "{:.2f}".format(float(five_min_hash[ticker][fiveMinTime]['open']))
            five_min_cur_high = "{:.2f}".format(float(five_min_hash[ticker][fiveMinTime]['high']))
            five_min_cur_low = "{:.2f}".format(float(five_min_hash[ticker][fiveMinTime]['low']))
            five_min_cur_close = "{:.2f}".format(float(five_min_hash[ticker][fiveMinTime]['close']))
            five_min_cur_vol = five_min_hash[ticker][fiveMinTime]['volume']

            tempString = f"('{ticker}','{cur_date}','{cur_open}','{cur_high}','{cur_low}','{cur_close}','{cur_vol}','{five_min_cur_open}','{five_min_cur_high}','{five_min_cur_low}','{five_min_cur_close}','{five_min_cur_vol}'),"
            insertString += tempString

        insertString = insertString[:-1]
        insertQuery = f"insert into usa_1min.stock_list_1min (ticker_ex,cur_date,cur_open,cur_high,cur_low,cur_close,cur_vol,5min_cur_open,5min_cur_high,5min_cur_low,5min_cur_close,5min_cur_vol) values {insertString} on duplicate key update cur_date = values(cur_date),cur_open = values(cur_open),cur_high = values(cur_high),cur_low = values(cur_low),cur_close = values(cur_close),cur_vol = values(cur_vol),5min_cur_open = values(5min_cur_open),5min_cur_high = values(5min_cur_high),5min_cur_low = values(5min_cur_low),5min_cur_close = values(5min_cur_close),5min_cur_vol = values(5min_cur_vol);"

        cursor.execute(insertQuery)
        cursor.close()
        connection.close()
    except Exception as e:
        #print(insertQuery)
        print("insertStockListMin Error :",e)


#insert data in stock_list table (daily)
def insertStockList():
    try:
        connection = connect_mariaDB()
        cursor = connection.cursor()
        tickers = day_hash.keys()

        insertString = ""
        for ticker in tickers:

            cur_date = list(day_hash[ticker].keys())[0]
            dateTime = cur_date
            cur_open =  "{:.2f}".format(float(day_hash[ticker][dateTime]['open']))
            cur_high =  "{:.2f}".format(float(day_hash[ticker][dateTime]['high']))
            cur_low =   "{:.2f}".format(float(day_hash[ticker][dateTime]['low']))
            cur_close = "{:.2f}".format(float(day_hash[ticker][dateTime]['close']))
            cur_vol = day_hash[ticker][dateTime]['volume']

            tempString = f"('{ticker}','{cur_date}','{cur_open}','{cur_high}','{cur_low}','{cur_close}','{cur_vol}'),"
            insertString += tempString

        insertString = insertString[:-1]
        insertQuery = f"insert into usa_daily.stock_list (ticker_ex,cur_date,cur_open,cur_high,cur_low,cur_close,cur_vol) values {insertString} on duplicate key update `cur_open(1)` = IF(cur_date = values(cur_date),`cur_open(1)`,cur_open),`cur_high(1)` = IF(cur_date = values(cur_date),`cur_high(1)`,cur_high),`cur_low(1)` = IF(cur_date = values(cur_date),`cur_low(1)`,cur_low),`cur_close(1)` = IF(cur_date = values(cur_date),`cur_close(1)`,cur_close),`cur_vol(1)` = IF(cur_date = values(cur_date),`cur_vol(1)`,cur_vol),prev_close = IF(cur_date = values(cur_date),prev_close,cur_close),cur_date = values(cur_date),cur_open = values(cur_open),cur_high = values(cur_high),cur_low = values(cur_low),cur_close = values(cur_close),cur_vol = values(cur_vol);"

        cursor.execute(insertQuery)
        cursor.close()
        connection.close()
    except Exception as e:
        #print(insertQuery)
        print("insertStockList  Error :",e)

#insert ticker OHLCV data into mariaDB(intraday + daily + bulk hist intraday)
def insertData(ticker,prefHash,tf,local_connection):

    global dbConfig
    try:

        #local_connection = mysql.connector.connect(**dbConfig,autocommit = True,buffered = True)
        insertCursor = local_connection.cursor()
        exchange = tickerDict[ticker]['exchange']
        exchCode = "nd" if exchange == "NASDAQ" else ("ny" if exchange == "NYSE" else ("mx" if exchange == "AMEX" else "ar"))

        insertString = ""
        dbName = "usa_" + tf
        for dateTime in list(prefHash[ticker].keys()):
            min_open = prefHash[ticker][dateTime]['open']
            min_high = prefHash[ticker][dateTime]['high']
            min_low = prefHash[ticker][dateTime]['low']
            min_close = prefHash[ticker][dateTime]['close']
            min_volume = prefHash[ticker][dateTime]['volume']
            tempString = f"('{dateTime}','{min_open}','{min_high}','{min_low}','{min_close}','{min_volume}','{min_volume}','{min_open}','{min_high}','{min_low}','{min_close}'),"
            insertString += tempString

        insertString = insertString[:-1]
        insertQuery = f"insert into {dbName}.`{ticker.lower()}_{exchCode}_{tf}` (date,open,high,low,close,volume,adj_vol,adj_open,adj_high,adj_low,adj_close) values {insertString} on duplicate key update open = values(open),high = values(high),low = values(low),close = values(close),volume = values(volume),adj_vol = values(adj_vol),adj_open = values(adj_open),adj_high = values(adj_high),adj_low = values(adj_low),adj_close = values(adj_close);"

        insertCursor.execute(insertQuery)
    except Exception as e:
        print("Insert Data Error:",ticker,":",e)
        try:
            createTable(insertCursor,ticker)
            insertCursor.execute(insertQuery)
        except Exception as e:
            print(insertQuery)
            print("Insert Data Retry Error :",ticker,":",e)
    finally:
        insertCursor.close()

#insert history data in intraday tables
#run in 30 tickers batch processing
def insertHistoryData(prefHash,tf):

    global dbConfig
    try:
        num_threads = 10
        connection = connect_mariaDB()

        for ticker in list(prefHash.keys()):
            insertData(ticker,prefHash,tf,connection)

        print(f"\n{tf} Data inserted Successfully.")
        connection.close()

    except Exception as e:
        print("Insert History Data Error :",e)


#insert 5min fields in stock_list_1min
def update5MinStockList():
    try:

        connection = connect_mariaDB()
        cursor = connection.cursor()
        tickers = five_min_hash.keys()

        insertString = ""
        for ticker in tickers:

            fiveMinTime = list(five_min_hash[ticker].keys())[0]
            five_min_date = fiveMinTime
            five_min_open = "{:.2f}".format(float(five_min_hash[ticker][fiveMinTime]['open']))
            five_min_high = "{:.2f}".format(float(five_min_hash[ticker][fiveMinTime]['high']))
            five_min_low = "{:.2f}".format(float(five_min_hash[ticker][fiveMinTime]['low']))
            five_min_close = "{:.2f}".format(float(five_min_hash[ticker][fiveMinTime]['close']))
            five_min_vol = five_min_hash[ticker][fiveMinTime]['volume']

            tempString = f"('{ticker.lower()}','{five_min_date}','{five_min_open}','{five_min_high}','{five_min_low}','{five_min_close}','{five_min_vol}'),"
            insertString += tempString

        insertString = insertString[:-1]
        insertQuery = f"insert into usa_1min.stock_list_1min (ticker_ex,5min_date,5min_open,5min_high,5min_low,5min_close,5min_vol) values {insertString} on duplicate key update 5min_date = values(5min_date),5min_open = values(5min_open),5min_high = values(5min_high),5min_low = values(5min_low),5min_close = values(5min_close),5min_vol = values(5min_vol);"
        cursor.execute(insertQuery)
        cursor.close()
        connection.close()

    except Exception as e:
        print("update5MinStockList Error :",e)

#get last run date
def getLastRunDate():
    try:
        connection = connect_mariaDB()
        lastRunDate = ""
        cursor = connection.cursor()
        query = "select max(5min_date) from usa_1min.stock_list_1min;"
        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        connection.close()
        if rows:
            for row in rows:
                lastRunDate = str(row[0])

    except Exception as e:
        print("fetch last run date error :",e)

    return lastRunDate


#store 5 min snapshots of whole day in 'bulk_five_min_hash'
def append_five_min_hash_to_bulk():

    global bulk_five_min_hash
    for ticker, data in five_min_hash.items():
        if ticker not in bulk_five_min_hash:
            bulk_five_min_hash[ticker] = {}

        # Deepcopy to ensure we store the current snapshot
        for timestamp, ohlcv in data.items():
            bulk_five_min_hash[ticker][timestamp] = copy.deepcopy(ohlcv)


def main():

    global min_hash,five_min_hash,isHist,day_hash,bulk_min_hash,bulk_five_min_hash,nextMarketStartDate,nextEodDate,isDst

    #fetch tickers
    tickers,tickers2 = getTickers()

    #create table for all tickers
    createStockListTable()
    createTable()
    print("Tables created.")

    #insert ticker_ex,name,exchange in stock_list tables.
    insertTickerData()

    startDate,endDate = parseArgs()
    #startDate = endDate = "2024-09-11"

    print("\nStart Date :",startDate)
    print("End   Date :",endDate)


    stringTickers = ""
    stringTickers2 = ""
    for ticker in tickers:
        tempString = ticker + ","
        stringTickers += tempString
    stringTickers = stringTickers[:-1]

    for ticker in tickers2:
        tempString2 = ticker + ","
        stringTickers2 += tempString2
    stringTickers2 = stringTickers2[:-1]

    start_date = datetime.strptime(startDate,"%Y-%m-%d")
    end_date = datetime.strptime(endDate,"%Y-%m-%d")

    st = datetime.now()
    print("script_start_time :",st)

    while start_date <= end_date:
        startDate = start_date.strftime("%Y-%m-%d")

        isDst = check_dst(startDate)

        bulk_min_hash = {}
        bulk_five_min_hash = {}

        #for intraday market(1min,5min data)
        if isIntraday:

            print("\nCurrent Date :",startDate)

            #check if its holiday,trading day or special session
            if isHoliday(startDate) == True:
                print(f"\n{startDate} is market holiday.")
                start_date += timedelta(days = 1)
                continue
            elif isHoliday(startDate) == False:
                startTime = startDate  + " 09:31:00"
                endTime   = startDate + " 16:00:00"
                fiveMinTick = startDate  + " 09:30:00"
            else:
                #special session
                print("Special Session :")
                startTime = str(isHoliday(startDate)).split(",")[0]
                fiveMinTick = startTime
                endTime = str(isHoliday(startDate)).split(",")[1]
                special_session_start_time = datetime.strptime(startTime,"%Y-%m-%d %H:%M:%S")
                startTime = (special_session_start_time + timedelta(minutes = 1)).strftime("%Y-%m-%d %H:%M:%S")


            print(f"startTime :{startTime}\nendTime : {endTime}")

            next_date = start_date +  timedelta(days = 1)
            nextDate = next_date.strftime("%Y-%m-%d")

            prev_date = start_date - timedelta(days = 1)
            prevDate = prev_date.strftime("%Y-%m-%d")

            while(isHoliday(nextDate) == True):
                next_date = next_date +  timedelta(days = 1)
                nextDate = next_date.strftime("%Y-%m-%d")
            nextMarketStartDate = nextDate + " 09:31:00"

            while(isHoliday(prevDate) == True):
                prev_date = prev_date - timedelta(days = 1)
                prevDate = prev_date.strftime("%Y-%m-%d")

            #if special session on next_date
            if (isHoliday(nextDate) != True and isHoliday(nextDate) != False):
                nextMarketStartDate = isHoliday(nextDate).split(",")[0]
                nextMarketStartDate = (datetime.strptime(nextMarketStartDate,"%Y-%m-%d %H:%M:%S")+ timedelta(minutes = 1)).strftime("%Y-%m-%d %H:%M:%S")


            print("\nCurrent Date :",startDate,"\nPrevious Date :",prevDate,"\nNext Date :",nextDate)

            #set market start and endTime
            realMarketStartTime = fiveMinTick   #09:30:00
            marketStartTime = startTime         #09:31:00
            marketEndTime = endTime             #16:00:00

            #check if lastrundate is equal to startDate
            lastRunDate = getLastRunDate()
            print("Last Run Date :",lastRunDate)

            #continue market from lastRunDate.
            if lastRunDate != "" and startDate == lastRunDate.split(" ")[0]:
                last_run_date = datetime.strptime(lastRunDate,"%Y-%m-%d %H:%M:%S")
                startTime = (last_run_date + timedelta(minutes = 1)).strftime("%Y-%m-%d %H:%M:%S")
                fiveMinTick = lastRunDate

            start_time = datetime.strptime(startTime,"%Y-%m-%d %H:%M:%S")
            end_time = datetime.strptime(endTime,"%Y-%m-%d %H:%M:%S")

            while start_time <= end_time:

                startTime = start_time.strftime("%Y-%m-%d %H:%M:%S")

                print("startTime :",startTime)

                #clear min_hash and five_min_hash in every 1min and five min respectively
                min_hash = {}

                #clear hash at 'postFiveMinTick',startTime + 1min after 5min tick: 09:16,09:21,09:26
                postFiveMinTick = (datetime.strptime(fiveMinTick,"%Y-%m-%d %H:%M:%S") + timedelta(minutes = 1)).strftime("%Y-%m-%d %H:%M:%S")
                if startTime == postFiveMinTick:
                    five_min_hash = {}

                #real time
                #curTime = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")

                #15 min delayed
                #curTime = (datetime.now(tz) - timedelta(days = 1) - timedelta(minutes = 15)).strftime("%Y-%m-%d %H:%M:%S")
                curTime = (datetime.now(tz) - timedelta(minutes = 15)).strftime("%Y-%m-%d %H:%M:%S")
                cur_time = datetime.strptime(curTime,"%Y-%m-%d %H:%M:%S")


                #if startDate is current system date,sleep until market starts
                real_market_start_time = datetime.strptime(realMarketStartTime,"%Y-%m-%d %H:%M:%S")
                if curTime.split(" ")[0] == startDate and cur_time <= real_market_start_time:
                    sleepTime = int(real_market_start_time.timestamp()) - int(cur_time.timestamp())
                    print("sleepTime:",sleepTime)
                    print()

                    for i in range(sleepTime,0,-1):
                        print(f"\rMarket will start in  {str(i)} seconds.",end = "")
                        sleep(1)

                    print("\nMarket Started.")

                    #set real market start date at 09:30:00
                    intradayUpdateInfo(realMarketStartTime,False,False,False)


                #fetch next min tick

                #real time
                #nextMin = (datetime.now(tz) + timedelta(minutes = 1)).strftime("%Y-%m-%dT%H:%M") + ":00Z"

                #15 min delayed
                nextMin = (datetime.now(tz) - timedelta(minutes = 14)).strftime("%Y-%m-%d %H:%M") + ":00"


                #after script catches up to current time.
                #sleep until next minute arrives
                if startTime == nextMin:

                    isHist = False
                    #real time
                    #cur_time = datetime.strptime(datetime.now(tz).strftime("%Y-%m-%dT%H:%M:%SZ"),"%Y-%m-%dT%H:%M:%SZ")

                    #15 min delayed
                    #cur_time = datetime.strptime((datetime.now(tz) - timedelta(days = 1) - timedelta(minutes = 15)).strftime("%Y-%m-%d %H:%M:%S"),"%Y-%m-%d %H:%M:%S")
                    cur_time = datetime.strptime((datetime.now(tz) - timedelta(minutes = 15)).strftime("%Y-%m-%d %H:%M:%S"),"%Y-%m-%d %H:%M:%S")
                    next_min = datetime.strptime(nextMin,"%Y-%m-%d %H:%M:%S")
                    sleepTime = int(next_min.timestamp()) - int(cur_time.timestamp()) + 1
                    print("sleepTime:",sleepTime)
                    print()

                    for i in range(sleepTime,0,-1):
                        print(f"\rsleeping for {str(i)} seconds.",end = "")
                        sleep(1)

                    print()

                current_tick = startTime.replace("T"," ").replace("Z","")
                print("\ncurrent Time :",current_tick)

                #get next fiveMinTick
                #FiveMinTick stores timestamp of next 5 min tick
                #example: it returns 09:20:00 for startTime 09:16:00 to 09:20:00
                fiveMinTick = getNextFiveMinTick(current_tick)

                #09:31:00 data is stored in 01:30:00 candle on alpaca and so on(same for every tick)

                if isDst:
                    data_tick = start_time + timedelta(minutes = 239)
                else:
                    data_tick = start_time + timedelta(minutes = 299)
                dataTick = data_tick.strftime("%Y-%m-%d %H:%M:%S")

                #get intrday data for tickers from dataTick to dataTick(inclusive) i.e for startTime
                data = getIntradayData(stringTickers,stringTickers2,dataTick,dataTick)
                for data_hash in data:
                    for ticker,ohlc_list in data_hash.items():
                        if ticker not in bulk_min_hash:
                            bulk_min_hash[ticker] = {}
                            if startTime not in bulk_min_hash[ticker]:
                                bulk_min_hash[ticker][current_tick] = {}

                        if ticker not in min_hash:
                            min_hash[ticker] = {}
                            if startTime not in  min_hash[ticker]:
                                min_hash[ticker][current_tick] = {}

                        for ohlc in ohlc_list:
                            Open = "{:.2f}".format(float(round(ohlc['o'],2)))
                            high = "{:.2f}".format(float(round(ohlc['h'],2)))
                            low = "{:.2f}".format(float(round(ohlc['l'],2)))
                            close = "{:.2f}".format(float(round(ohlc['c'],2)))
                            volume = int(ohlc['v'])

                            min_hash[ticker][current_tick] = {"open" : Open, "high" : high , "low" : low, "close" : close , "volume" : volume}
                            bulk_min_hash[ticker][current_tick] = {"open" : Open, "high" : high , "low" : low, "close" : close , "volume" : volume}
                            generateFiveMinOHLC(ticker)

                print("min_hash :",min_hash)
                timestamp = list(list(min_hash.values())[0].keys())[0]

                tf = "1min"
                thread_insert = threading.Thread(target = insertHistoryData,args = (min_hash,tf))
                thread_insert.start()

                #print(f"Fetched 1 min data for {timestamp}")
                if (not isHist) or (update_symbol_list == 1):

                    #insert min_hash into mariaDB after every minute.
                    insertStockListMin()

                if startTime == fiveMinTick:
                    print("\nfive_min_hash :",five_min_hash)
                    print("5 min data generated.")

                    #insert 5 min data
                    tf = "5min"
                    thread_5insert = threading.Thread(target = insertHistoryData,args = (five_min_hash,tf))
                    thread_5insert.start()

                    #append_five_min_hash_to_bulk()

                    if (not isHist) or (update_symbol_list == 1):

                        #update '5min' fields in symbol_list_1min
                        update5MinStockList()

                print("Total Tickers :",len(list(set(min_hash.keys()))))
                start_time += timedelta(minutes = 1)

            #while end (startTime <= endTime)

            '''if isHist:

                #insert 1 min data
                tf = "1min"
                print("\nInserting 1 min data...")
                insertHistoryData(bulk_min_hash,tf)

                #insert 5 min data
                tf = "5min"
                print("\nInserting 5 min data...")
                insertHistoryData(bulk_five_min_hash,tf)'''

        if isDaily:

            day_hash = {}
            isHist = True
            startDate = (start_date).strftime("%Y-%m-%d")
            curDate = datetime.now(tz).strftime("%Y-%m-%d")

            #eod is updated on 10:30 IST next morning
            if startDate == curDate:
                startDate = (datetime.now(tz)- timedelta(days = 1)).strftime("%Y-%m-%d")
                isHist = False

            print("\nEOD Date :",startDate)

            #check if its holiday
            if isHoliday(startDate) == True:
                print(f"\n{startDate} is market holiday.")
                start_date += timedelta(days = 1)
                continue

            next_date = datetime.strptime(startDate,"%Y-%m-%d") +  timedelta(days = 1)
            nextDate = next_date.strftime("%Y-%m-%d")

            while(isHoliday(nextDate) == True):
                next_date = next_date +  timedelta(days = 1)
                nextDate = next_date.strftime("%Y-%m-%d")

            nextEodDate = nextDate + " 17:20:00"

            data = get_EOD_Data(stringTickers,stringTickers2,startDate)

            if len(data) > 0:
                for data_hash in data:
                    for ticker,ohlc_list in data_hash.items():
                        if ticker not in day_hash:
                            day_hash[ticker] = {}
                            if startDate not in  day_hash[ticker]:
                                day_hash[ticker][startDate] = {}
                        for ohlc in ohlc_list:
                            Open = "{:.2f}".format(float(round(ohlc['o'],2)))
                            high = "{:.2f}".format(float(round(ohlc['h'],2)))
                            low = "{:.2f}".format(float(round(ohlc['l'],2)))
                            close = "{:.2f}".format(float(round(ohlc['c'],2)))
                            volume = int(ohlc['v'])
                            day_hash[ticker][startDate] = {"open" : Open, "high" : high , "low" : low, "close" : close , "volume" : volume}

                print("day_hash:",day_hash)
                print("Total Tickers :",len(day_hash))
                if (not isHist) or (update_symbol_list == 1):
                    insertStockList()
                    #eodUpdateInfo(startDate)

                tf = "daily"
                insertHistoryData(day_hash,tf)

            print(f"\nMarket Ended for {startDate}")

        start_date = start_date + timedelta(days = 1)

    #no need to join the threads,as they will still run after main program ends.
    #note: but if we set thread as daemon it will be killed alongside with main function
    '''if isIntraday:
        thread_insert.join()
        thread_5insert.join()'''
    et = datetime.now()
    print("\nTotal time taken by script :",et-st)

main()
