# import datetime
# import holidays

# d = datetime.datetime.strftime(datetime.datetime.today(), '%Y-%m-%d')
# print(datetime.date(*map(lambda n: int(n), d.split('-'))))

# tw_holidays = holidays.country_holidays("TW")  # this is a dict
# the below is the same, but takes a string:
# us_holidays = holidays.country_holidays('US')  # this is a dict
# print(tw_holidays)
# print(date(2023, 2, 28) in tw_holidays)

from bs4 import BeautifulSoup
import re
import orjson
from util import StockUtil, MysqlUtil

# def remove_old_files():
#   import os, re
#   import datetime
#   today = datetime.datetime.strftime(datetime.datetime.now() - datetime.timedelta(days = 3), '%Y-%m-%d')
#   for f in os.listdir('.'):
#     if m := re.search(r'^list.*(\d{4}-\d{2}-\d{2})_\d+.txt$', f):
#       if m.group(1) < today:
#         os.remove(f)

if __name__ == "__main__":
  stock_util = StockUtil(None, "tw")
  stock_util.fetch_stagnating_stocks_tw()

  # import requests
  # proxies={'http': 'http://rjgyxuyy:iluj9yhr6m2q@2.56.119.93:5074'}
  # r = requests.get('https://goodinfo.tw/tw/StockDetail.asp?STOCK_ID=1101', proxies=proxies)
  # r.encoding = "utf-8"
  # print(r.text)