# us_stock_data
The script is used to fetch 10k+ US stocks 1 min snapshot data using Alpaca Paper Trading API and insert it in mariaDB.(15 min delayed data).
It fetches 1 min candles,generates 5 min candles from them and store it in database.
It also fetches EOD (1 day interval)data.
It includes stocks from NASDAQ,NYSE,AMEX and NYSE ARCA and also includes highly traded ETFs like SPY,QQQ.
Before running this script:
1.Make sure to change database names according to your requirement.
2.Create Alpaca trading account(https://app.alpaca.markets/signup)(only Email verification needed for paper trading) and generate Paper trading API and secret key for stock data.
and paste them in usEquity.py
3.Create polygon account ( https://polygon.io/ )(only email verfication) for using holiday API,generate API key and paste it in usa_holidays.py
4.Make sure MySQL is installed properly and make changes in connect_mariaDB() function according to your config.
4.Run usa_holidays.py to make sure the script will skip fetching data on US market Holidays.
#######################################################################################################################################
#
#                                                                          usEquity.py
#
#         Description  :      This script is used to fetch US stocks intraday and eod data using alpaca paper trading API(15 min delayed data)
#                             for real time data alpaca trading plus subscription is required.
#                             since passing 10k+ tickers in API is not possible,we are passing two batches of 5000+ tickers each for every min.
#
#                                  For Regular run :
#
#                                             Intraday:
#                                                  usage: python3.9 usEquity.py -intraday
#
#                                                   on every minute:
#                                                    i) update stock_list_1min table
#                                                   ii) update 5 min OHLC on every min and insert it in 5min tables on every 5 min tick.
#                                                  iii) insert 1min data in history 1min table for current minute
#                                                   iv) sleep until next min data arrives
#                                              EOD:
#                                                  usage: python3.9 usEquity.py -eod
#
#                                                    i) update stock_list table
#                                                   ii) insert EOD data into history table
#
#                                              Note : intraday data is available 15 min late for regular run so we adjust it in our code
#                                                    (real time 1min adjustment is available but commented as of now in this script)
#                                                     eg: 09:31 candle data will be available on 9:46,but just like tradingview our 09:31 is their 09:30 in API
#                                                     EOD data is updated next morning(11.30 IST)(10:30 IST in Daylight savings time)so for regular run,
#                                                     '-intraday' uses current date while '-eod' uses yesterday's date.if script is ran without any argument
#                                                     it will run for intraday by default.
#
#                                  For History run :(45-50 min for one day data)
#
#                                             For updating stock_list_1min(intraday) or updating stock_list(eod) pass '-symbol_list' in history run.
#                                             No need to pass it in regular run.
#
#                                             Intraday:
#                                                  usage: python3.9 usEquity.py -sd 2024-09-23 -ed 2024-09-24 -intraday -symbol_list
#                                                         (-sd and -ed arguments dates are inclusive,same date can be passed in both)
#                                                   i) Iterate minute by minute and insert history data via thread.
#                                                  ii) After every 5 minutes tick,insert 5 min history data via thread.
#                                                 iii) If symbol_list is parsed,update stock_list_1min on every minute.
#                                                iiii) Iterate to next date and clear all the hashes used.
#
#
#                                              EOD:
#                                                  usage: python3.9 usEquity.py -sd 2024-09-23 -ed 2024-09-24 -eod
#                                                   i) fetch and insert EOD data into history table.
#                                                  ii) if '-symbol_list' is parsed,update stock_list table.
#
#
####################################################################################################################################################
 
