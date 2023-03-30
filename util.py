from bs4 import BeautifulSoup
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, wait, as_completed, ALL_COMPLETED
from datetime import datetime, timedelta, date
from http import cookies
import json, orjson
import os
import random
import re
import requests
import shutil
import sys
import time
import traceback
from urllib.parse import urlparse, parse_qs, urlencode
from user_agent import generate_user_agent
import pymysql
from selenium import webdriver
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
from io import StringIO


class Util:
  RETRY = 3
  REQUEST_TIMEOUT = 25
  FETCH_TIMEOUT_FROM = 1
  FETCH_TIMEOUT_TO = 2
  PROXY_PAGE = None
  MAX_PROXY_PAGE = 20
  PROXIES = [ 'rjgyxuyy:iluj9yhr6m2q@2.56.119.93:5074', 'rjgyxuyy:iluj9yhr6m2q@185.199.229.156:7492', 'rjgyxuyy:iluj9yhr6m2q@185.199.228.220:7300', 'rjgyxuyy:iluj9yhr6m2q@185.199.231.45:8382', 'rjgyxuyy:iluj9yhr6m2q@188.74.210.207:6286', 'rjgyxuyy:iluj9yhr6m2q@188.74.183.10:8279', 'rjgyxuyy:iluj9yhr6m2q@188.74.210.21:6100', 'rjgyxuyy:iluj9yhr6m2q@45.155.68.129:8133', 'rjgyxuyy:iluj9yhr6m2q@154.95.36.199:6893', 'rjgyxuyy:iluj9yhr6m2q@45.94.47.66:8110' ]

  def __init__(self):
    pass

  def nested_dict(self):
    return defaultdict(self.nested_dict)

  def print_nested_dict(self, nd):
    import json
    print(json.loads(json.dumps(nd)))

  def connect(self, host='http://google.com'):
    import urllib.request
    try:
      urllib.request.urlopen(host)
      return True
    except:
      return False

  def get_proxy(self):
    while not len(self.PROXIES):
      self.update_proxy()

    random.seed(time.time())
    offset = random.randint(0, len(self.PROXIES) - 1)
    proxies = { "http": self.PROXIES[offset] }
    del self.PROXIES[offset]

    return proxies

  def update_proxy(self, proxy_page: int = 1):
    import json

    self.PROXIES = []
    if self.PROXY_PAGE:
      self.PROXY_PAGE += 1
    elif proxy_page:
      self.PROXY_PAGE = proxy_page
    else:
      self.PROXY_PAGE = 1
    self.PROXY_PAGE = self.PROXY_PAGE % self.MAX_PROXY_PAGE if self.PROXY_PAGE > self.MAX_PROXY_PAGE else self.PROXY_PAGE

    retry = 0
    while retry < self.RETRY:
      try:
        proxy_url = "https://proxylist.geonode.com/api/proxy-list?limit=500&page={}&sort_by=lastChecked&sort_type=desc".format(self.PROXY_PAGE)
        print("Getting proxies from {}".format(proxy_url))
        html = self.html_get(proxy_url, use_proxy=False)

        proxies_json = json.loads(html)
        for proxy in proxies_json['data']:
          if proxy['protocols'][0] in [ 'http', 'https', 'socks4', 'socks5' ] and re.match("\d+\.\d+\.\d+\.\d+", proxy['ip'], re.I) and re.match("\d+", proxy['port'], re.I):
            self.PROXIES.append("{}://{}:{}".format(proxy['protocols'][0], proxy['ip'], proxy['port']))

        if not len(self.PROXIES):
          retry += 1
          time.sleep(0.3)
          continue

        break
      except:
        traceback.print_exc()
        retry += 1
        time.sleep(0.3)

    if retry >= self.RETRY:
      print("Failed updating proxies.")
      sys.exit(0)

  def old_update_proxy(self):
    self.PROXIES = []
    retry = 0
    while retry < self.RETRY:
      try:
        driver = webdriver.Chrome(ChromeDriverManager().install())
        # driver = webdriver.Chrome('/Users/decken/Dropbox/Stocks/chromedriver')
        driver.implicitly_wait(10)
        driver.get('https://free-proxy-list.net/')

        driver.find_element(By.CLASS_NAME, 'fa-clipboard').click()
        for line in driver.find_element(By.CLASS_NAME, 'modal-body').find_element(By.CLASS_NAME, 'form-control').get_attribute('value').split('\n'):
          line = line.strip()
          if re.match("\d+\.\d+\.\d+\.\d+:\d+", line, re.I):
            self.PROXIES.append(line)

        break
      except:
        driver.quit()
        traceback.print_exc()
        retry += 1
        print('Retrying...' + str(retry))

  def html_get(self, url, stock_name=None, all_stock_names=None, proxy=None, text=True, use_proxy=False, verify_item=None, header=None):
    if not header:
      header = {}
    if all_stock_names:
      header = { 'Referer': 'https://www.marketwatch.com/{}/'.format(all_stock_names[random.randint(0, len(all_stock_names) - 1)]), 'Host': 'www.marketwatch.com' }

    print(f"=== html_get: {url} ===")

    counter = 0
    connection_error = 0
    while counter < self.RETRY:
      counter += 1
      try:
        header['User-Agent'] = generate_user_agent()
        proxies = None
        if proxy:
          proxies = proxy
        elif use_proxy:
          proxies = self.get_proxy()
        params = {
          'headers': header,
          'timeout': self.REQUEST_TIMEOUT ,
        }
        if proxies:
          params['proxies'] = proxies
          print("PROXY => {:}".format(proxies))
        res = requests.get(url, **params)
        res.encoding = "utf-8"
        if verify_item and verify_item not in res.text:
          print("The fetched page is wrong.")
          continue
        break
      except requests.ConnectionError:
        connection_error += 1
        print("Connection Error: Sleep for {} seconds...".format(450 * connection_error))
        time.sleep(450 * connection_error)
      except requests.TooManyRedirects:
        time.sleep(0.3)
        pass
      except:
        sys.stderr.write("*** {} ***\n".format(url))
        traceback.print_exc()
        sys.stderr.write("--- {} ---\n".format(url))
        time.sleep(10 * counter)

    if counter >= self.RETRY:
      print('Failed retrying!')
      if stock_name:
        self.add_failed_stock(stock_name)

      if connection_error >= self.RETRY:
        print("Stopped due to Connection Error.")
        sys.exit(1)
        # return 'connection_error'
      return -1
    else:
      if text:
        return res.text
      else:
        return res.content

  def mdy_to_ymd(self, dt, sep='/'):
    dt = dt.split(sep)
    return "{}-{}-{}".format(dt[2], dt[0], dt[1])

  def twymd_to_ymd(self, dt, sep='/'):
    dt = dt.split(sep)
    return "{}-{}-{}".format(int(dt[0]) + 1911, dt[1], dt[2])

  def ymd_to_twymd(self, dt, sep='-'):
    dt = dt.split(sep)
    return "{}/{}/{}".format(int(dt[0]) - 1911, dt[1], dt[2])

  def transform_ymd(self, dt, sep='/'):
    import re
    dt = re.split(sep, dt)
    return "{}-{}-{}".format(dt[0], dt[1], dt[2])

  def last_weekdays(self):
    import datetime
    today = datetime.date.today()
    idx = (today.weekday() + 1) % 7
    sat = today - datetime.timedelta(7 + idx - 6)
    sun = today - datetime.timedelta(7 + idx - 7)

    return sat, sun


class MysqlUtil(Util):
  def __init__(self, conn=None, cursor=None, stock_type=''):
    Util.__init__(self)
    self.conn = conn
    self.cursor = cursor
    self.stock_type = stock_type

  def create_sql_conn(self):
    import pymysql

    try:
      with open('cred.json') as json_file:
        cred = json.load(json_file)
      conn = pymysql.connect(host=cred['mysql']['ip'], port=cred['mysql']['port'], user=cred['mysql']['username'], passwd=cred['mysql']['password'], db=cred['mysql']['database'] + (f"_{self.stock_type}" if self.stock_type else ''), charset="utf8")
      cursor = conn.cursor(pymysql.cursors.DictCursor)

      return (conn, cursor)
    except:
      sys.exit("Error: unable to create database connection.")

  def close_sql_conn(self, conn, cursor):
    if cursor:
      cursor.close()
    if conn:
      conn.close()

  def execute_sql(self, sql, values):
    if not (self.conn and self.cursor):
      (self.conn, self.cursor) = self.create_sql_conn()
    self.cursor.execute(sql, values)
    self.conn.commit()


class StockUtil(Util):
  insert_index_volume_sql = 'INSERT IGNORE INTO index_volume (`date`, stock_id, volume, `index`, index_open, index_low, index_high) VALUES(%s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE `index` = VALUES(`index`), index_open = VALUES(index_open), index_low = VALUES(index_low), index_high = VALUES(index_high), volume = VALUES(volume)'
  insert_stock_name_sql = 'INSERT IGNORE INTO stock_list (stock_name, stock_name_tw) VALUES(%s, %s)'
  stock_failed_ignored_tw = ("2321", "6873")
  qualified_days_ratio = 0.6
  rs_ratio = 0.3
  year_min_ratio = 0.45
  num_days = 30
  volume_ratio = 1.25
  ratio_ma20 = 0.92
  ratio_ma50 = 0.92
  ratio_ma150 = 0.92
  ratio_ma200 = 0.92
  index_lowest = 2
  index_highest = 35
  volume_lowest = 100000
  stagnate_ratio = 0.25
  retrospect_days = 400
  order_number = 2
  reIsNumber = re.compile(r'^([0-9]*[.])?[0-9]+$', re.S | re.UNICODE)

  def __init__(self, export_file=None, stock_type='', conn=None, cursor=None):
    Util.__init__(self)

    self.export_file = export_file
    self.stock_type = stock_type

    if self.stock_type == 'tw':
      self.index_highest = 105
    else:
      self.index_highest = 55

    if conn and cursor:
      (self.conn, self.cursor) = (conn, cursor)
    else:
      mysql_util = MysqlUtil(stock_type=self.stock_type)
      (self.conn, self.cursor) = mysql_util.create_sql_conn()

    for d in ['GICS', 'GICS/finished']:
      if not os.path.exists(d):
        os.makedirs(d)

    from dbutils.pooled_db import PooledDB
    import json
    import pymysql

    with open('cred.json') as json_file:
      cred = json.load(json_file)
    self.pool = PooledDB(
      creator=pymysql,
      maxconnections=5,
      mincached=5,
      maxcached=0,
      maxusage=None,
      blocking=True,
      host=cred['mysql']['ip'],
      port=cred['mysql']['port'],
      user=cred['mysql']['username'],
      passwd=cred['mysql']['password'],
      db=cred['mysql']['database'] + (f"_{stock_type}" if stock_type else ''),
      use_unicode=True,
      charset='utf8'
    )

  def able_to_show(self, stock_name, days=2):
    if not hasattr(self, 'good_dates'):
      good_date_sql = '''
        SELECT DISTINCT(date) good_dates
        FROM good_stock gs
        WHERE date < DATE(NOW())
        ORDER BY date DESC
        LIMIT {}
      '''
      self.good_dates = []
      self.cursor.execute(good_date_sql.format(self.conn.escape(days + 1)))
      for row in self.cursor.fetchall():
        self.good_dates.append(datetime.strftime(row['good_dates'], '%Y-%m-%d'))
      self.good_dates = self.good_dates[::-1]

    self.cursor.execute('''
      SELECT date
      FROM good_stock gs
      WHERE stock_name = {} AND date BETWEEN {} AND {}
      ORDER BY date ASC
    '''.format(
      *map(lambda s: self.conn.escape(s), (stock_name, self.good_dates[0], self.good_dates[-1]))
    ))
    stock_good_dates = []
    for row in self.cursor.fetchall():
      stock_good_dates.append(datetime.strftime(row['date'], '%Y-%m-%d'))
    if len(stock_good_dates) == 0 or stock_good_dates[0] == self.good_dates[0]:
      return True
    return False

    # good_sql = '''
    #   SELECT gs.*
    #   FROM good_stock gs
    #   INNER JOIN (
    #     SELECT DISTINCT(date) FROM good_stock WHERE date <> DATE(NOW()) ORDER BY date DESC LIMIT {}
    #   ) t ON t.date = gs.date
    # '''
    # # SELECT DISTINCT(date) FROM good_stock ORDER BY date DESC LIMIT {} OFFSET 1

    # if not hasattr(self, 'good_stocks'):
    #   self.good_stocks = {}
    #   self.cursor.execute(good_sql.format(self.conn.escape(days + 1)))
    #   for row in self.cursor.fetchall():
    #     self.good_stocks[row['stock_name']] = 1

    # return True if stock_name in self.good_stocks else False

  def transform_m_b_to_number(self, num):
    ch_to_num = {
      'k': 1000,
      'm': 1000000,
      'b': 1000000000,
    }

    sign = 1
    if '(' in num:
      sign = -1
    num = num.replace(',', '').replace('(', '').replace(')', '').lower()

    for ch in ch_to_num.keys():
      if ch in num:
        return float(num.replace(ch, ''))*ch_to_num[ch]*sign

    if not num.isdigit():
      return False

    return float(num)*sign

  def get_stock_id(self, stock_name, conn=None, cursor=None, stock_name_tw=None):
    select_stock_id_sql = 'SELECT id FROM stock_list WHERE stock_name = %s'

    conn = conn if conn else self.conn
    cursor = cursor if cursor else self.cursor

    cursor.execute(select_stock_id_sql, (stock_name))
    stock_id = cursor.fetchone()
    if stock_id and 'id' in stock_id:
      stock_id = stock_id['id']
    else:
      cursor.execute(self.insert_stock_name_sql, (stock_name, stock_name_tw))
      conn.commit()
      stock_id = cursor.lastrowid

    return stock_id

  def get_stock_info_xueqiu(self, stock_name, use_proxy=True):
    from hanziconv import HanziConv

    headers = {
      'X-Requested-With': 'XMLHttpRequest',
      'Referer': 'http://xueqiu.com/p/ZH010389',
      'User-Agent': 'Mozilla/5.0 (Windows NT 6.2; WOW64; rv:38.0) Gecko/20100101 Firefox/38.0',
      'Host': 'xueqiu.com',
      # 'Connection':'keep-alive',
      # 'Accept':'*/*',
      'cookie':'s=iabht2os.1dgjn9z; xq_a_token=02a16c8dd2d87980d1b3ddced673bd6a74288bde; xq_r_token=024b1e233fea42dd2e0a74832bde2c914ed30e79; __utma=1.2130135756.1433017807.1433017807.1433017807.1;'
      '__utmc=1; __utmz=1.1433017807.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); Hm_lvt_1db88642e346389874251b5a1eded6e3=1433017809; Hm_lpvt_1db88642e346389874251b5a1eded6e3=1433017809'
    }

    counter = 0
    while counter < self.RETRY:
      counter += 1

      try:
        proxies = {}
        if use_proxy:
          proxies = self.get_proxy()
          print("PROXY => {:}".format(proxies))
        res = requests.get("https://xueqiu.com/S/" + stock_name, headers=headers, proxies=proxies, timeout=self.REQUEST_TIMEOUT)
        reGetStockInfo = re.compile(r"profile-detail.*?\">(.*?)<", re.S | re.UNICODE)
        for stockInfo in reGetStockInfo.findall(res.text):
          return HanziConv.toTraditional(stockInfo)
      except:
        traceback.print_exc()
        time.sleep(1)

    return ''

  def insert_index_volume(self, date, stock_id, index, volume, conn=None, cursor=None):
    data = [date, stock_id, volume]
    data.extend(map(lambda i: index[i], ['index', 'index_open', 'index_low', 'index_high']))
    conn = conn if conn else self.conn
    cursor = cursor if cursor else self.cursor
    cursor.execute(self.insert_index_volume_sql, tuple(data))
    conn.commit()

  def insert_manually_blocked_stocks(self):
    try:
      shutil.copyfile('/home/decken/Dropbox2/Dropbox/manual.txt', 'manual.txt')
    except:
      traceback.print_exc()
    sql = []
    file = open("manual.txt", "r")
    for stock in file.read().splitlines():
      if not stock:
        continue
      sql.append("('{}', NOW(), 'manual')".format(stock.upper()))
    if not sql:
      return

    mysql_util = MysqlUtil(stock_type=self.stock_type)
    (conn, cursor) = mysql_util.create_sql_conn()
    insert_stock_sql = "INSERT IGNORE INTO `blocked_stock` (`stock_name`, `update_time`, `type`) VALUES\n" + ", \n".join(sql) + ';'
    cursor.execute(insert_stock_sql)
    conn.commit()

  def retrieve_all_stocks(self):
    url = 'https://www.marketwatch.com/tools/markets/stocks/country/united-states/'

    page = 1
    file_counter = 1
    stocks = []
    while True:
      try:
        current_url = url + str(page)
        print("Fetching >>> {}".format(current_url))
        html = self.html_get(current_url, verify_item='mw-logo-fp')
        soup = BeautifulSoup(html, 'html.parser')
        has_data = False

        for tr in soup.find('tbody').find_all("tr"):
          for stock in re.compile(r"\s+\(([a-zA-Z]+?)\)$", re.S | re.UNICODE).findall(tr.find('a').text):
            has_data = True
            stocks.append(stock)
            if len(stocks) >= 8000:
              import csv
              file = open('GICS/stocks-Xueqiu-all-' + str(file_counter) + '.csv', 'w+', newline='', encoding="utf8")
              csvCursor = csv.writer(file)
              csvCursor.writerow(['Xueqiu-all'])
              for stock in stocks:
                csvCursor.writerow(['https://xueqiu.com/S/' + stock])
              file.close()

              stocks = []
              file_counter += 1
        page += 1
        if not has_data:
          break
      except:
        traceback.print_exc()
        time.sleep(1)

    if stocks:
      import csv
      file = open('GICS/stocks-Xueqiu-all-' + str(file_counter) + '.csv', 'w+', newline='', encoding="utf8")
      csvCursor = csv.writer(file)
      csvCursor.writerow(['Xueqiu-all'])
      for stock in stocks:
        csvCursor.writerow(['https://xueqiu.com/S/' + stock])
      file.close()

  def stock_info_fetch_tw_20230308(self):
    if self.stock_type != 'tw':
      return

    self.cursor.execute('''
      SELECT  stock_name
      FROM    stock_list
      WHERE   stock_info IS NULL
    ''')
    for stock_name in self.cursor.fetchall():
      url  = f"https://tw.stock.yahoo.com/quote/{stock_name['stock_name']}.TW/profile"
      html = self.html_get(url, verify_item='ybar-logo', header={"referer": "https://tw.stock.yahoo.com/"})
      soup = BeautifulSoup(html, 'html.parser')
      for div in soup.find_all('div'):
        if re.match(r'^掛.*?牌.*?日.*?期.*?', div.text.strip()):
          print(div.find('div').text.strip())
          ipo_date = '-'.join(div.find('div').text.strip().split('/'))
          continue
        if re.match(r'^產.*?業.*?類.*?別', div.text.strip()):
          print(div.find('div').text.strip())
          stock_category = div.find('div').text.strip()
          continue
        if re.match(r'^主.*?要.*?業.*?務', div.text.strip()):
          print(div.find('div').text.strip())
          stock_info = ', '.join(re.split(r'\r?\n', div.find('div').text.strip()))
          break

      self.cursor.execute('UPDATE stock_list SET stock_info = %s, stock_category = %s, ipo_date = %s WHERE stock_name = %s', (stock_info, stock_category, ipo_date, stock_name['stock_name']))
      self.conn.commit()

      time.sleep(3)

  def stock_info_retrieve(self, stock_id):
    if not hasattr(self, 'stock_info'):
      self.cursor.execute('''
        SELECT  *
        FROM    stock_list
      ''')
      self.stock_info = {
        stock_info['id']: stock_info
        for stock_info in self.cursor.fetchall()
      }

    return self.stock_info[stock_id]

  def renew_index_volume_tw(self):
    self.retrieve_all_stocks_tw_20230308()
    self.stock_info_fetch_tw_20230308()
    self.get_index_volume_tw_20230308()
    self.retrospect_ma(stock_id=None, days=self.retrospect_days)

  def retrieve_all_stocks_tw_20230308(self):
    html = self.html_get("https://isin.twse.com.tw/isin/C_public.jsp?strMode=2")
    soup = BeautifulSoup(html, 'html.parser')
    for tr in soup.findAll('table')[1].find_all("tr"):
      stock = tr.findAll('td')[0].text
      stock_split = list(map(lambda s: s.strip(), re.split('\s+', stock)))
      if re.match(r'[1-9]\d+', stock_split[0]):
        self.get_stock_id(stock_split[0], None, None, stock_split[1])

  def get_index_volume_tw_20230308(self, skip_stock=True):
    self.cursor.execute('''
      SELECT  stock_name
      FROM    stock_list
      WHERE   id NOT IN (
        SELECT  DISTINCT(stock_id)
        FROM    index_volume
      )
    ''')
    self.new_stocks = [ stock_name['stock_name'] for stock_name in self.cursor.fetchall() ]

    month_dates = []
    first = date.today()
    month_dates.append(first.strftime("%Y%m%d"))
    for _ in range(12):
      first = first.replace(day=1)
      last_month = first - timedelta(days=1)
      month_dates.append(last_month.strftime("%Y%m%d"))
      first = last_month

    header = {
      'Host': 'www.twse.com.tw',
      'Origin': 'https://www.twse.com.tw',
      'Referer': 'https://www.twse.com.tw/zh/page/trading/exchange/STOCK_DAY.html',
      'Cookie': '_ga=GA1.3.546622630.1678318304; _gid=GA1.3.1279139493.1678318304; JSESSIONID=BEBCFA0A46E863715CDD199B26CF8BD6; _gat=1'
    }
    retry = self.RETRY + 3
    for stock_name in self.new_stocks:
      stock_id = self.get_stock_id(stock_name)
      print("\nFetching {}({})".format(stock_name, stock_id))

      failed = False
      for yyyymmdd in month_dates:
        url = f"https://www.twse.com.tw/zh/exchangeReport/STOCK_DAY?response=json&date={yyyymmdd}&stockNo={stock_name}&random={random.uniform(self.FETCH_TIMEOUT_FROM, self.FETCH_TIMEOUT_TO)}"

        counter = 0
        while counter < retry:
          counter += 1

          print("\nGetting {}".format(url))
          html = self.html_get(url.format(stock_name), stock_name=stock_name, header=header)
          if html != -1:
            try:
              stock_info = json.loads(html)
              if stock_info['fields'][0] != '日期':
                continue
              rows = []
              for row in stock_info['data']:
                dt = self.twymd_to_ymd(row[0])
                idx = float(row[6]) if self.reIsNumber.match(row[6]) else 0
                idx_open = float(row[3]) if self.reIsNumber.match(row[3]) else 0
                idx_low = float(row[5]) if self.reIsNumber.match(row[5]) else 0
                idx_high = float(row[4]) if self.reIsNumber.match(row[4]) else 0
                volume = self.transform_m_b_to_number(str(row[1])) if row[1].replace(',', '').isdigit() else 0
                rows.append((dt, stock_id, volume, idx, idx_open, idx_low, idx_high))
              self.cursor.executemany(self.insert_index_volume_sql, rows)
              self.conn.commit()
              break
            except:
              traceback.print_exc()
          time.sleep(2.5)
        if counter >= retry:
          print("*** Failed: {} ***\n".format(stock_name))
          self.add_failed_stock(stock_name)
          failed = True
          break
        time.sleep(2.5)

      if failed:
        time.sleep(2.5)
        continue

  def renew_index_volume(self):
    self.retrieve_all_stocks_20230225()
    self.get_index_volume_20230225()
    self.retrospect_ma(stock_id=None, days=self.retrospect_days)

  def retrieve_all_stocks_20230225(self):
    reGetStock = re.compile(r'"/stocks/.+?/?">([A-Z\.]+)</a>', re.S | re.UNICODE)
    urls = ['https://stockanalysis.com/list/nyse-stocks/', 'https://stockanalysis.com/list/nasdaq-stocks/']
    stocks = set()

    for url in urls:
      driver = webdriver.Chrome(ChromeDriverManager().install())
      driver.get(url)
      driver.implicitly_wait(10)

      while 1:
        try:
          for stock in reGetStock.findall(driver.page_source):
            stocks.add(stock)
          driver.find_element(By.XPATH, '//span[text()="Next"]').click()
          time.sleep(6)
        except:
          driver.quit()
          break

      driver.quit()

    self.cursor.executemany(self.insert_stock_name_sql, [(stock) for stock in stocks])
    self.conn.commit()

    return stocks

  def get_index_volume_20230225(self, skip_stock=True):
    self.cursor.execute('''
      SELECT  stock_name
      FROM    stock_list
    ''')
    all_stock_names = [ stock_name['stock_name'] for stock_name in self.cursor.fetchall() ]
    self.cursor.execute('''
      SELECT  stock_name
      FROM    stock_list
      WHERE   id NOT IN (
        SELECT  DISTINCT(stock_id)
        FROM    index_volume
      )
    ''')
    self.new_stocks = [ stock_name['stock_name'] for stock_name in self.cursor.fetchall() ]
    url = 'https://www.marketwatch.com/investing/stock/{0}/download-data'

    for stock_name in self.new_stocks:
      print("\nFetching {}".format(stock_name))

      download_link = None
      counter = 0
      while counter < self.RETRY:
        counter += 1

        html = self.html_get(url.format(stock_name), stock_name=stock_name, all_stock_names=all_stock_names, verify_item='MW_Masthead_Logo')
        if html != -1:
          soup = BeautifulSoup(html, 'html.parser')
          for a_href in soup.findAll("a"):
            if a_href.text == 'Download Data (.csv)':
              download_link = a_href['href']
              break
        if download_link:
          break
      if counter >= self.RETRY:
        print("*** Failed: {} ***\n".format(stock_name))
        self.add_failed_stock(stock_name)
        time.sleep(0.3)
        continue

      params = parse_qs(urlparse(download_link).query)
      params['startdate'][0] = (datetime.now() - timedelta(days=self.retrospect_days)).strftime("%m/%d/%Y 00:00:00")
      params['enddate'][0] = datetime.now().strftime("%m/%d/%Y 23:59:59")
      download_link = "{}?{}".format(download_link[:download_link.find('?')], urlencode(params, doseq=True))
      print("download_link: {}".format(download_link))

      stock_id = self.get_stock_id(stock_name)
      print("stock_id: {}".format(stock_id))

      try:
        content = self.html_get(download_link, stock_name=stock_name, all_stock_names=all_stock_names)
        df = pd.read_csv(StringIO(content))
        rows = []
        for _, row in df.iterrows():
          dt = self.mdy_to_ymd(row['Date'])
          idx = float(str(row['Close']).replace('$', '').replace(',', ''))
          idx_open = float(str(row['Open']).replace('$', '').replace(',', ''))
          idx_low = float(str(row['Low']).replace('$', '').replace(',', ''))
          idx_high = float(str(row['High']).replace('$', '').replace(',', ''))
          volume = self.transform_m_b_to_number(str(row['Volume']).replace(',', '')) if 'Volume' in row else 0
          rows.append((dt, stock_id, volume, idx, idx_open, idx_low, idx_high))
        self.cursor.executemany(self.insert_index_volume_sql, rows)
        self.conn.commit()
      except:
        sys.stderr.write("*** {} ***\n".format(stock_name))
        traceback.print_exc()
        self.add_failed_stock(stock_name)
        sys.stderr.write("--- {} ---\n".format(stock_name))

      time.sleep(0.2)

  def stock_info_fetch_20230326(self):
    if self.stock_type == 'tw':
      return

    from selenium import webdriver
    from webdriver_manager.chrome import ChromeDriverManager

    self.cursor.execute('''
      SELECT  stock_name
      FROM    stock_list
      WHERE   ipo_date IS NULL
    ''')
    for stock_name in self.cursor.fetchall():
      url = f"https://www.futunn.com/hk/stock/{stock_name['stock_name']}-US/company-profile"
      # driver = webdriver.Chrome(ChromeDriverManager().install())
      # def interceptor(request):
      #   del request.headers['Referer']  # Delete the header first
      #   request.headers['Referer'] = "https://www.futunn.com/"
      # # Set the interceptor on the driver
      # driver.request_interceptor = interceptor

      print(f"=== {url} ===")
      # driver.get(url)
      # driver.implicitly_wait(10)
      html = self.html_get(url, verify_item='nn-logo', header={"referer": "https://www.futunn.com/"})
      # soup = BeautifulSoup(driver.page_source, 'html.parser')
      soup = BeautifulSoup(html, 'html.parser')
      ipo_date_found = False
      for div in soup.find_all('div'):
        if re.match(r'^上.*?市.*?日.*?期', div.text.strip()):
          ipo_date_found = True
          continue
        if ipo_date_found:
          ipo_date = div.text.strip().replace('/', '-')
          break

      self.cursor.execute('UPDATE stock_list SET ipo_date = %s WHERE stock_name = %s', (ipo_date, stock_name['stock_name']))
      self.conn.commit()

      # driver.quit()
      time.sleep(10)

  def extract_expenditure_revenue(self, stock_name):
    res = self.html_get('https://www.marketwatch.com/investing/stock/' + stock_name + '/financials/income/quarter')
    if res == -1:
      return False

    revenue_values = []
    soup = BeautifulSoup(res, 'html.parser')
    try:
      for tr in soup.find('tbody', {'class': "table__body"}).find_all("tr", {"class": "table__row"}):
        title = tr.find('td', {"class": "fixed--column"}).find('div', {'class': 'fixed--cell'})
        if 'Sales/Revenue' == title.text.strip():
          revenue_sum = 0.0
          revenue_counter = 0
          for value in tr.find_all('td', {"class": "overflow__cell"}):
            if "fixed--column" in value["class"]:
              continue

            v = value.text.strip()
            if v:
              revenue_values.append(v)

            number = self.transform_m_b_to_number(v)
            if not number:
              continue

            revenue_sum += number
            revenue_counter += 1

          print('===Sales/Revenue===')
          print(revenue_counter)
          print(revenue_sum)
          print(self.transform_m_b_to_number(revenue_values[-1]))
          print(revenue_sum / revenue_counter)
          print('---Sales/Revenue---')
    except:
      traceback.print_exc()

  def get_earning_released_date(self, stock_name):
    html = self.html_get("https://finance.yahoo.com/quote/{}".format(stock_name))
    if html != -1:
      soup = BeautifulSoup(html, 'html.parser')
      try:
        return soup.find("td", {"data-test": "EARNINGS_DATE-value"}).text
      except:
        traceback.print_exc()

  def get_stocks_from_url(self, url):
    from selenium import webdriver
    from webdriver_manager.chrome import ChromeDriverManager

    page = 1
    stocks = {}
    while 1:
      try:
        print('{}&page={}'.format(url, page))

        driver = webdriver.Chrome(ChromeDriverManager().install())
        driver.get(url + '&page=' + str(page))
        driver.implicitly_wait(10)

        if '暂无数据' in driver.page_source:
          break

        soup = BeautifulSoup(driver.page_source, 'html.parser')
        success = 0
        for ele in soup.find('table', {'class': 'portfolio'}).find_all('tr', {'class': ['odd', 'even']}):
          stocks[ele.select('td')[0].select('a')[0].text.strip()] = 1
          success = 1

        if not success:
          time.sleep(random.uniform(self.FETCH_TIMEOUT_FROM, self.FETCH_TIMEOUT_TO))
          driver.quit()
          continue

        page += 1
        time.sleep(random.uniform(self.FETCH_TIMEOUT_FROM, self.FETCH_TIMEOUT_TO))
        driver.quit()
      except:
        traceback.print_exc()
        driver.quit()

    return stocks.keys()

  def GICS_csvs(self, categories):
    if not categories:
      print("'categories' is required.")
      return -1

    for group in categories.keys():
      stocks = self.get_stocks_from_url(categories[group])

      import csv
      file = open('GICS/stocks-Xueqiu-' + group + '.csv', 'w+', newline='', encoding="utf8")
      csvCursor = csv.writer(file)
      csvCursor.writerow(['Xueqiu-' + group])
      for stock in stocks:
        csvCursor.writerow(['https://xueqiu.com/S/' + stock])
      file.close()

  def get_Xueqiu_categories(self):
    from hanziconv import HanziConv
    from selenium import webdriver
    from webdriver_manager.chrome import ChromeDriverManager

    url = 'https://xueqiu.com/hq#exchange=US&industry=3_2&firstName=3&page=1'
    while 1:
      try:
        driver = webdriver.Chrome(ChromeDriverManager().install())
        driver.get(url)
        driver.implicitly_wait(10)

        soup = BeautifulSoup(driver.page_source, 'html.parser')
        categories = {}
        for ele in soup.find_all('i', { 'class': 'list-style' }):
          if re.search("明星股", ele.parent.text):
            for li in ele.parent.find_all('li'):
              key = HanziConv.toTraditional(li.text).strip()
              link = "https://xueqiu.com/hq{}".format(li.select('a')[0]['href'].strip())
              categories[key] = link

        driver.quit()
        break
      except:
        traceback.print_exc()
        driver.quit()

    self.GICS_csvs(categories)

  def line_notify(self, group=None, good_stock_names=None, msg=None):
    from datetime import date

    with open('cred.json') as json_file:
      cred = json.load(json_file)
      token = cred['line']['token']

    headers = {
      "Authorization": "Bearer {}".format(token),
      "Content-Type": "application/x-www-form-urlencoded"
    }
    if msg:
      pass
    elif group:
      msg = "[{}] {}".format(group, ', '.join(good_stock_names))
    elif good_stock_names:
      msg = "{}".format(', '.join(good_stock_names))
    elif not (self.test or self.stagnate):
      msg = "=== {} ===".format(date.today())
    else:
      return False
    params = { "message": msg }

    try:
      r = requests.post("https://notify-api.line.me/api/notify", headers=headers, params=params)
    except:
      print("Line notification failed.")
      traceback.print_exc()

  def get_stock_days(self, num_days=20):
    skip_dates = [ '2020-11-26', '2020-12-25', '2021-01-01', '2021-01-18', '2021-02-15', '2021-04-02', '2021-05-31', '2021-07-05', '2021-09-06', '2021-11-25', '2021-12-24', '2022-01-17', '2022-02-21', '2022-04-15', '2022-05-30', '2022-06-20', '2022-07-04', '2022-09-05', '2022-11-24', '2022-12-26', '2023-01-02', '2023-01-16', '2023-02-20', '2023-04-07', '2023-05-29', '2023-06-19', '2023-07-04', '2023-09-04', '2023-11-23', '2023-12-25' ]
    skip_dates_tw = [ '2023-01-02', '2023-01-18', '2023-01-19', '2023-01-20', '2023-01-23', '2023-01-24', '2023-01-25', '2023-01-26', '2023-01-27', '2023-02-27', '2023-02-28', '2023-04-03', '2023-04-04', '2023-04-05', '2023-05-01', '2023-06-22', '2023-06-23', '2023-09-29', '2023-10-09', '2023-10-10', '2024-01-01' ]

    from datetime import datetime, timedelta
    if (self.stock_type != "tw" and datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d') in skip_dates) or (self.stock_type == "tw" and datetime.strftime(datetime.now(), '%Y-%m-%d') in skip_dates_tw):
      return []

    import pandas as pd
    from pandas.tseries.offsets import BDay
    if not self.test:
      today = self.today = datetime.today()
      self.today_ymd = datetime.strftime(self.today, '%Y-%m-%d')
    else:
      # today = pd.to_datetime(input("Enter date: "))
      today = pd.to_datetime('2023-02-08')
      today += BDay(1)
      self.today = today
      self.today_ymd = datetime.strftime(self.today, '%Y-%m-%d')
    days = []
    shift = 0
    while True:
      nds = num_days + shift
      for delta in range(nds, (-1 if self.stock_type == 'tw' else 0), -1):
        d = datetime.strftime(today - BDay(delta), '%Y-%m-%d')
        if (self.stock_type != 'tw' and d not in skip_dates) or (self.stock_type == 'tw' and d not in skip_dates_tw and d <= self.today_ymd):
          days.append(d)

      if len(days) < num_days:
        shift += 1
        days = []
      else:
        break

    return days

  def get_blocked_stocks(self):
    select_blocked_sql = '''
      SELECT      stock_name
      FROM        blocked_stock
    '''

    blocked_stocks = []
    self.cursor.execute(select_blocked_sql)
    for row in self.cursor.fetchall():
      blocked_stocks.append(row['stock_name'])

    return blocked_stocks

  def add_blocked_stock(self, stock_name, conn=None, cursor=None):
    insert_blocked_sql = 'INSERT INTO blocked_stock (stock_name, update_time) VALUES (%s, NOW()) ON DUPLICATE KEY UPDATE update_time = NOW()'
    conn = conn if conn else self.conn
    cursor = cursor if cursor else self.cursor
    cursor.execute(insert_blocked_sql, (stock_name))
    conn.commit()

  def add_failed_stock(self, stock_name, conn=None, cursor=None):
    insert_failed_sql = 'INSERT INTO failed_stock (stock_name, update_time) VALUES (%s, NOW()) ON DUPLICATE KEY UPDATE update_time = NOW()'
    conn = conn if conn else self.conn
    cursor = cursor if cursor else self.cursor
    cursor.execute(insert_failed_sql, (stock_name))
    conn.commit()

  def compute_ma(self, ma, stock_id, date):
    select_sql_ma = 'SELECT COUNT(*) days, SUM(`index`) `sum` FROM (SELECT `index` FROM index_volume WHERE stock_id = %s AND date <= %s ORDER BY `date` DESC LIMIT %s) AS ma'

    self.cursor.execute(select_sql_ma, (stock_id, date, ma))
    row = self.cursor.fetchone()
    return row['sum'] / ma if row['days'] == ma else 0

  def insert_ma(self, stock_id, date, ma=None, conn=None, cursor=None):
    if not ma:
      ma = {}
    for ma_days in [5, 10, 20, 60, 50, 150, 200]:
      if ma_days not in ma:
        ma[ma_days] = self.compute_ma(ma_days, stock_id, date)

    update_ma_sql = 'UPDATE index_volume SET ma_5 = %s, ma_10 = %s, ma_20 = %s, ma_60 = %s, ma_50 = %s, ma_150 = %s, ma_200 = %s WHERE stock_id = %s AND date = %s'
    conn = conn if conn else self.conn
    cursor = cursor if cursor else self.cursor
    cursor.execute(update_ma_sql, (ma[5], ma[10], ma[20], ma[60], ma[50], ma[150], ma[200], stock_id, date))
    conn.commit()

  def retrospect_ma(self, stock_id=None, days=None):
    # if not days:
    #   days = self.get_stock_days()
    # elif isinstance(days, int):
    #   days = self.get_stock_days(days)

    zero_ma_dates = self.retrieve_zero_ma_date(stock_id)

    # stock_id_sql = 'WHERE stock_id = {}'.format(self.conn.escape(stock_id)) if stock_id else ''
    # self.cursor.execute('''
    #     SELECT DISTINCT(stock_id), sl.stock_name
    #     FROM index_volume iv
    #     INNER JOIN stock_list sl ON sl.`id` = iv.`stock_id`
    #     # LIMIT 8200
    #     # LIMIT 18446744073709551615 OFFSET 8200
    #     {}
    # '''.format(stock_id_sql))
    # for row in self.cursor.fetchall():
    for stock_id in zero_ma_dates:
      # stock_id = row['stock_id']
      # if hasattr(self, 'new_stocks') and stock_id not in self.new_stocks:
      #     print('Skipped computing ma for {}...'.format(stock_id))
      #     continue
      # print('=== Computing ma: {}({}) ==='.format(row['stock_name'], stock_id))
      print('=== Computing ma: {} ==='.format(stock_id))
      # for dt in list(filter(lambda d: d <= zero_ma_date[stock_id], days)):
      for dt in zero_ma_dates[stock_id].split(','):
      # for dt in days:
        self.insert_ma(stock_id, dt)

  def retrieve_zero_ma_date(self, stock_id=None):
    self.cursor.execute('''
      # SELECT stock_id, MAX(`date`) zero_date
      SELECT stock_id, GROUP_CONCAT(`date`) zero_dates
      FROM index_volume
      WHERE (ma_5 = 0 OR ma_10 = 0 OR ma_20 = 0 OR ma_60 = 0 OR ma_50 = 0 OR ma_150 = 0 OR ma_200 = 0)
      {}
      GROUP BY stock_id
    '''.format('AND stock_id = ' + self.conn.escape(stock_id) if stock_id else ''))

    return { row['stock_id']: row['zero_dates'] for row in self.cursor.fetchall() }

  def compute_avg_volume(self, stock_id=None, conn=None, cursor=None, days=50):
    conn = conn if conn else self.conn
    cursor = cursor if cursor else self.cursor

    cursor.execute('''
      SELECT AVG(volume) avg_volume
      FROM (
        SELECT volume
        FROM index_volume
        WHERE stock_id = %s
        AND `date` < %s
        ORDER BY date DESC
        LIMIT %s
      ) t
    ''', (stock_id, self.today_ymd, days))
    row = cursor.fetchone()

    return row['avg_volume'] if row['avg_volume'] else 0

  def get_one_year_max_min_index(self, stock_id):
    self.cursor.execute('''
      SELECT MAX(`index`) max_index, MIN(`index`) min_index
      FROM (
        SELECT `index`
        FROM index_volume
        WHERE stock_id = %s
        AND `date` < %s
        ORDER BY `date` DESC
        LIMIT 260
      ) t
    ''', (stock_id, self.today_ymd))
    row = self.cursor.fetchone()

    return (row['max_index'], row['min_index'])

  def qualified_year_max_min(self, stock_id, index):
    year_max, year_min = self.get_one_year_max_min_index(stock_id)
    print(f"year_min: {year_min}, year_max: {year_max}, index must be higher than: {((year_max - year_min) * self.year_min_ratio + year_min)}")
    # return abs(index - year_max) / year_max <= 0.25 and (index - year_min) / year_min >= 0.3
    return True if year_min != 0 and index >= ((year_max - year_min) * self.year_min_ratio + year_min) else False

  def get_index_volume_from_html(self, html, day, stock_website=None):
    soup = BeautifulSoup(html, 'html.parser')
    if stock_website == 'yahoo':
      counter = 0
    else:
      counter = 1

    while True:
      try:
        if stock_website == 'yahoo':
          tr = soup.find("div", {"id": "Col1-1-HistoricalDataTable-Proxy"}).find("tbody").select('tr')[counter]
          while '股息' in tr.text:
            counter += 1
            tr = soup.find("div", {"id": "Col1-1-HistoricalDataTable-Proxy"}).find("tbody").select('tr')[counter]

          page_day = self.transform_ymd(tr.select('td')[0].select('span')[0].text, '年|月|日')
          if page_day == day:
            pass
          elif page_day > day:
            counter += 1
            continue
          else:
            return -1, None, None

          index_open, index_high, index_low, index = map(lambda i: float(tr.select('td')[i].select('span')[0].text.replace('$', '').replace(',', '')), [1, 2, 3, 4])
          volume = tr.select('td')[6].select('span')[0].text.replace(',', '')
          volume = self.transform_m_b_to_number(volume)

          return True, { 'index_open': index_open, 'index_high': index_high, 'index_low': index_low, 'index': index }, volume
        elif self.stock_type == 'tw':
          stock_info = json.loads(html)
          twymd_day = self.ymd_to_twymd(day)
          stock_info_dict = {}
          for row in stock_info['data']:
            stock_info_dict[row[0]] = row[1:]
          if stock_info['fields'][0] != '日期' or twymd_day not in stock_info_dict:
            return -1, None, None

          idx = float(stock_info_dict[twymd_day][5]) if self.reIsNumber.match(stock_info_dict[twymd_day][5]) else 0
          idx_open = float(stock_info_dict[twymd_day][2]) if self.reIsNumber.match(stock_info_dict[twymd_day][2]) else 0
          idx_low = float(stock_info_dict[twymd_day][4]) if self.reIsNumber.match(stock_info_dict[twymd_day][4]) else 0
          idx_high = float(stock_info_dict[twymd_day][3]) if self.reIsNumber.match(stock_info_dict[twymd_day][3]) else 0
          volume = self.transform_m_b_to_number(str(stock_info_dict[twymd_day][0])) if stock_info_dict[twymd_day][0].replace(',', '').isdigit() else 0
          return True, { 'index_open': idx_open, 'index_high': idx_high, 'index_low': idx_low, 'index': idx }, volume
        else:
          tr = soup.find("div", {"class": "download-data"}).select('tr')[counter]

          page_day = self.mdy_to_ymd(tr.select('td')[0].select('div')[0].text, '/')
          if page_day == day:
            pass
          elif page_day > day:
            counter += 1
            continue
          else:
            return -1, None, None

          index_open, index_high, index_low, index = map(lambda i: float(tr.select('td')[i].text.replace('$', '').replace(',', '')), [1, 2, 3, 4])
          volume = tr.select('td')[5].text.replace(',', '')
          volume = self.transform_m_b_to_number(volume)

          return True, { 'index_open': index_open, 'index_high': index_high, 'index_low': index_low, 'index': index }, volume
      except:
        traceback.print_exc()
        return -2, None, None

  def yield_stock_names(self, file_name):
    import csv
    with open(file_name, encoding="utf8", newline='') as csvfile:
      rows = csv.reader(csvfile, delimiter=',')

      headers = next(rows, None)
      stock_list = []
      for row in rows:
        for _, stock_url in enumerate(row, start=0):
          if not stock_url or '/' not in stock_url:
            continue

          stock_name = stock_url.split('/')[-1]
          yield stock_name

  def compute_min_max_avg(self, stock_name):
    get_index_sql = '''
      SELECT MIN(`index`) min, MAX(`index`) max , AVG(`index`) avg
      FROM (
        SELECT `index`
        FROM index_volume
        WHERE stock_id = %s
        AND date < %s
        ORDER BY date DESC
        LIMIT %s
      ) t
    '''

    stock_id = self.get_stock_id(stock_name)
    self.cursor.execute(get_index_sql, (stock_id, self.today_ymd, self.num_days))
    return self.cursor.fetchone()

  def is_stagnate_stock(self, stock_name):
    row = self.compute_min_max_avg(stock_name)

    return True if row['min'] and row['min'] >= row['avg'] * (1 - self.stagnate_ratio) and row['max'] <= row['avg'] * (1 + self.stagnate_ratio) else False

  def find_stagnate_stocks(self, file_name):
    blocked_files = ['消費信貸', '綜閤消費者服務', '互助儲蓄銀行與', '綜閤金融服務', '保險', '股權房地産投資', '商業銀行', '抵押房地産投資', '房地産管理和開', '資本市場', '股權房地産投資']
    if list(filter(lambda f: f in file_name, blocked_files)):
      return

    from datetime import datetime
    today = datetime.now().strftime("%Y-%m-%d") # '2021-06-19'
    tmp_num_days = self.num_days
    self.num_days = 90
    if not hasattr(self, 'today'):
      self.today = datetime.today()
    blocked_stocks = self.get_blocked_stocks()
    stocks = []
    rows = []

    # last_sat, last_sun = self.last_weekdays()
    for stock_name in self.yield_stock_names(file_name):
      if stock_name in blocked_stocks:
        continue

      stock_id = self.get_stock_id(stock_name)
      is_qualified = 1
      if self.stagnate:
        get_sql = '''
          SELECT 1
          FROM stagnating_stock
          WHERE date BETWEEN CURDATE() - INTERVAL 18 DAY AND CURDATE() - INTERVAL 6 DAY
          AND WEEKDAY(date) IN (5, 6)
          AND stock_name = %s
          AND type = 'weekly'
        '''
        self.cursor.execute(get_sql, (stock_name))
        is_stagnating_stock = self.cursor.fetchone()

        get_sql = '''
          SELECT SUM(`index` > ma_50), SUM(ma_50 > IFNULL(ma_150, 0)), SUM(ma_50 > IFNULL(ma_200, 0))#, SUM(IFNULL(ma_150, 0) > IFNULL(ma_200, 0))
          FROM (
            SELECT `index`, ma_50, ma_150, ma_200
            FROM index_volume
            WHERE stock_id = %s
            ORDER BY date DESC
            LIMIT %s
          ) t
        '''
        self.cursor.execute(get_sql, (stock_id, self.num_days))
        criterions = self.cursor.fetchone()
        for criterion in criterions.keys():
          if criterions[criterion] <= self.num_days / 2:
            is_qualified = 0
            break

      year_max, year_min = self.get_one_year_max_min_index(stock_id)
      row = self.compute_min_max_avg(stock_name)
      if is_qualified and row and year_min and abs(row['avg'] - year_min) / year_min < self.year_min_ratio:
        if not (self.stagnate and is_stagnating_stock):
          stocks.append(stock_name)
        rows.append((today, stock_name, 'weekly' if self.stagnate else 'daily'))

    insert_sql = 'INSERT IGNORE INTO stagnating_stock (`date`, stock_name, `type`) VALUES(%s, %s, %s)'
    self.cursor.executemany(insert_sql, rows)
    self.conn.commit()

    self.num_days = tmp_num_days
    return stocks

  def fetch_stagnating_stocks_tw(self):
    if self.stock_type != 'tw':
      return

    self.cursor.execute(f'''
      SELECT      sl.stock_name, sl.stock_name_tw, iv.*
      FROM        index_volume iv
      INNER JOIN  stock_list sl
        ON        sl.id = iv.stock_id
      WHERE       date = (SELECT MAX(date) FROM index_volume)
      ORDER BY    stock_id
    ''')
    # self.line_notify('', '', "=== TW Get Stagnating Stocks ===")
    for stock in self.cursor.fetchall():
      if stock["index"] > stock["ma_50"] and stock["ma_50"] > stock["ma_150"]:
        print(f"{stock['stock_name']}({stock['stock_name_tw']})")
    # self.line_notify('', '', "--- TW Get Stagnating Stocks ---")


  def dispatch_stocks(self, file_name, stock_website=None):
    days = self.get_stock_days(self.num_days)
    blocked_stocks = self.get_blocked_stocks()

    with ThreadPoolExecutor(max_workers=5) as executor:
      future_to_stock_name = {}
      for stock_name in self.yield_stock_names(file_name):
        if stock_name in blocked_stocks:
          continue

        print(f'Adding {stock_name} into futures...')
        future_to_stock_name = { executor.submit(self.compute_stock, stock_name, days): stock_name }
      for future in as_completed(future_to_stock_name):
        stock_name = future_to_stock_name[future]
        try:
          print(f'{stock_name}: {future.result()}')
        except Exception:
          print(f'{stock_name} generated an exception.')
          traceback.print_exc()
        # else:
        #     print('%r page is %d bytes' % (url, len(data)))
      # done, not_done = wait(futures, return_when=ALL_COMPLETED)
      # print(done.pop().result())

  def compute_stock(self, stock_name, days):
    print(f"\n*** {stock_name} ***")
    conn = self.pool.connection()
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    stock_id = self.get_stock_id(stock_name, conn, cursor)

    stocks_data = {}
    cursor.execute("SELECT `date`, `index`, volume, ma_5, ma_10, ma_20, ma_60, ma_50, ma_150, ma_200 FROM index_volume WHERE stock_id = %s AND `date` >= %s AND `date` <= %s", (stock_id, days[0], days[-1]))
    for row in cursor.fetchall():
      if stock_name not in stocks_data:
        stocks_data[stock_name] = { row['date']: {} }
      elif row['date'] not in stocks_data[stock_name]:
        stocks_data[stock_name][row['date']] = {}

      for key in ['index', 'volume',  'ma_5', 'ma_10', 'ma_20', 'ma_60', 'ma_50', 'ma_150', 'ma_200']:
        stocks_data[stock_name][row['date']][key] = row[key]

    url = "https://www.marketwatch.com/investing/stock/{}/download-data".format(stock_name)

    avg_volume = self.compute_avg_volume(stock_id, conn, cursor)
    volumes_list = []
    successful_days = 0
    failed_days = 0
    day_counter = 0
    first_day_success = 0
    blocked = 0
    qualified_days = 0
    html = None
    for day in days[::-1]:
      success = 0
      day_counter += 1
      counter = 0
      if stock_name in stocks_data and day in stocks_data[stock_name]:
        volumes_list.append(float(stocks_data[stock_name][day]['volume']))
        successful_days += 1
      else:
        print("{} {}: {}".format('(From cached html)' if html and html != 'connection_error' else '', day, url))
        while counter < self.RETRY:
          counter += 1

          html = html if html and html != 'connection_error' else self.html_get(url, stock_name=stock_name, verify_item='MW_Masthead_Logo')
          if html == 'connection_error':
            connection_error = 1
          elif html != -1:
            connection_error = 0

            success, index_dict, volume = self.get_index_volume_from_html(html, day)
            if success is not True:
              counter = self.RETRY
              break

            volumes_list.append(volume)
            if stock_name not in stocks_data:
              stocks_data[stock_name] = { day: {} }
            elif day not in stocks_data[stock_name]:
              stocks_data[stock_name][day] = {}
            stocks_data[stock_name][day]['index'] = index_dict['index']
            stocks_data[stock_name][day]['volume'] = volume

            self.insert_index_volume(day, stock_id, index_dict, volume, conn, cursor)
            successful_days += 1
            break

      if counter >= self.RETRY:
        print('Not Found: {} => {}'.format(stock_name, day))
        if success == -1:
          self.add_failed_stock(stock_name, conn, cursor)

        failed_days += 1
        if failed_days >= 3 and failed_days > successful_days and not connection_error and not first_day_success:
          blocked = 1
          break
      else:
        if day_counter == 1:
          first_day_success = 1

        print("\n==={} {}".format(stock_name, day))
        print(stocks_data[stock_name][day])
        print("---{} {}\n".format(stock_name, day))

    if blocked and not self.test:
      print("Add {} into blocked list".format(stock_name))
      self.add_blocked_stock(stock_name)
      return

    day_keys = sorted(stocks_data[stock_name].keys()) if stock_name in stocks_data else []
    for day in day_keys:
      ma = {}
      for ma_days in [5, 10, 20, 60, 50, 150, 200]:
        if ma_days not in ma or ma[ma_days] == 0:
          ma[ma_days] = stocks_data[stock_name][day]['ma_{}'.format(ma_days)] = self.compute_ma(ma_days, stock_id, day)
      self.insert_ma(stock_id, day, ma, conn, cursor)

      if stocks_data[stock_name][day]['volume'] <= float(avg_volume) * self.volume_ratio and self.qualified_year_max_min(stock_id, stocks_data[stock_name][day]['index']) and stocks_data[stock_name][day]['index'] >= stocks_data[stock_name][day]['ma_50'] * self.ratio_ma50 and (stocks_data[stock_name][day]['ma_50'] >= stocks_data[stock_name][day]['ma_150'] * self.ratio_ma150 or stocks_data[stock_name][day]['ma_50'] >= stocks_data[stock_name][day]['ma_200'] * self.ratio_ma200):
        qualified_days += 1

    volume_base = sum(volumes_list[1:]) / (len(days) - 1)
    if volume_base > 0:
      print('volume percent: {}'.format(volumes_list[0] / volume_base))

    if len(day_keys) >= self.num_days / 2:
      if stocks_data[stock_name][day_keys[0]]['index'] == 0:
        return

      print('today: {}'.format(stocks_data[stock_name][day_keys[-1]]['index']))
      if day_keys[-2] in stocks_data[stock_name]:
        print('yesterday: {}'.format(stocks_data[stock_name][day_keys[-2]]['index']))
      print('first day: {}'.format(stocks_data[stock_name][day_keys[0]]['index']))
      print('qualified_days: {}'.format(qualified_days))

      if stocks_data[stock_name][day_keys[-1]]['volume'] <= float(avg_volume) * self.volume_ratio and qualified_days >= len(day_keys) * self.qualified_days_ratio:
        if self.is_stagnate_stock(stock_name) and (not self.good(stock_name) or self.test):
          if not self.test:
            cursor.execute("INSERT IGNORE INTO good_stock (date, stock_name) VALUES (%s, %s)", (self.today_ymd, stock_name))
            conn.commit()

    print('---' + stock_name + '---')

  def fetch_dividend_info_tw(self, stock_name):
    # url_dividend = "https://www.twse.com.tw/zh/exchangeReport/TWT48U"
    url_dividend = f"https://tw.stock.yahoo.com/quote/{stock_name}.TW/dividend"
    html = self.html_get(url_dividend, verify_item='ybar-logo', header={"referer": "https://tw.stock.yahoo.com/"})
    soup = BeautifulSoup(html, 'html.parser')
    try:
      dividend_date = soup.find('div', {'class': "table-body-wrapper"}).find('ul').find('li').findAll('div')[6].text.replace('/', '-')
    except:
      return None
    if not re.match(r'^\d{4}-\d{2}-\d{2}$', dividend_date):
      # self.line_notify('', '', f"!!! Yahoo TW Stock Dividend Date Format might have changed ({url_dividend}) !!!")
      return None
    else:
      self.cursor.execute('UPDATE stock_list SET dividend_date = %s WHERE stock_name = %s', (dividend_date, str(stock_name)))
      self.conn.commit()

      dividend_date_split = dividend_date.split('-')
      if dividend_date_split[0] == str(date.today().year):
        dividend_date = f"{dividend_date_split[1]}-{dividend_date_split[2]}"
      elif dividend_date_split[0] < str(date.today().year):
        dividend_date = None
      return dividend_date
    # dividend_json = orjson.loads(self.html_get(url_dividend))
    # dividend_info = {}
    # for stock in dividend_json["data"]:
    #   tw_ymd, month, day = re.match(r'(\d+)年(\d+)月(\d+)日', stock[0]).group(1, 2, 3)
    #   tw_ymd = int(tw_ymd) + 1911
    #   if tw_ymd == date.today().year:
    #     tw_ymd = ''
    #   dividend_info[stock[1]] = '-'.join(map(lambda s: str(s), filter(None, (tw_ymd, month, day))))
    # return dividend_info

  # def get_stock_daily(self, file_name, stock_website=None):
  def get_stock_daily(self, stock_names, stock_website=None):
    days = self.days = self.get_stock_days(self.num_days) # ['2023-01-12', '2023-01-13', '2023-01-17', '2023-01-18', ...]
    blocked_stocks = self.get_blocked_stocks()
    good_stock_names, all_good_stock_names = [], []
    self.rs = 0
    self.rs_counter = 0

    for stock_name in stock_names:
      if stock_name in blocked_stocks:
        continue

      dividend_info = None
      if self.stock_type == 'tw':
        dividend_info = self.fetch_dividend_info_tw(stock_name)

      print("\n*** {} ***".format(stock_name))
      stock_id = self.get_stock_id(stock_name)
      if self.stock_type == 'tw':
        stock_info = self.stock_info_retrieve(stock_id)
        try:
          d0 = date(*map(int, stock_info['ipo_date'].split('-')))
          d1 = date(*map(int, self.today_ymd.split('-')))
          ipo_delta_days = (d1 - d0).days
        except:
          ipo_delta_days = 366

      # self.insert_ma(stock_id, '2022-01-14')
      # continue

      stocks_data = {}
      self.cursor.execute("SELECT `date`, `index`, volume, ma_5, ma_10, ma_20, ma_60, ma_50, ma_150, ma_200 FROM index_volume WHERE stock_id = %s AND `date` >= %s AND `date` <= %s", (stock_id, days[0], days[-1]))
      for row in self.cursor.fetchall():
        if stock_name not in stocks_data:
          stocks_data[stock_name] = { row['date']: {} }
        elif row['date'] not in stocks_data[stock_name]:
          stocks_data[stock_name][row['date']] = {}

        for key in ['index', 'volume',  'ma_5', 'ma_10', 'ma_20', 'ma_60', 'ma_50', 'ma_150', 'ma_200']:
          stocks_data[stock_name][row['date']][key] = row[key]

      header = None
      if stock_website == 'yahoo':
        url = "https://hk.finance.yahoo.com/quote/{}/history".format(stock_name)
      elif self.stock_type == 'tw':
        days_last = datetime(*map(lambda c: int(c), days[-1].split('-')))
        url_pattern = "https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY?response=json&date={}&stockNo={}&random={}"
        url = url_pattern.format(days_last.strftime("%Y%m%d"), stock_name, random.uniform(self.FETCH_TIMEOUT_FROM, self.FETCH_TIMEOUT_TO))
        header = {
          "Accept": "*/*",
          "Accept-Encoding": "gzip, deflate, br",
          "Accept-Language": ("zh-TW,zh;q=0.9", "en-US,en;q=0.9")[random.randint(0, 1)],
          "Connection": "keep-alive",
          # "Content-Length": "40", # failed
          "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
          # "Cookie": (
          #   "_ga=GA1.3.1270294213.1678751796; _gat=1; _gid=GA1.3.102096223.1678751796; JSESSIONID=88D50E11E14E68A033FEDAE3F5EC3C74",
          #   "_ga=GA1.3.546622630.1678318304; _gid=GA1.3.1131421049.1678719413; JSESSIONID=52580D51170388FDFDDABB4BA8D8D9E0; _gat=1",
          #   "JSESSIONID=89109E9EF04AE722EABD2306C29A0974; _ga=GA1.3.1566680994.1678752890; _gid=GA1.3.1643576225.1678752890; _gat=1",
          #   "JSESSIONID=12F266E19FE262E72EC15C7FA6767D03; _ga=GA1.3.595896652.1678752971; _gid=GA1.3.598567431.1678752971; _gat=1",
          #   "JSESSIONID=8B3E5066842A70B9AD86508EFD8B21DF; _ga=GA1.3.2086219825.1678753013; _gid=GA1.3.868061642.1678753013; _gat=1"
          # )[random.randint(0, 4)],
          "Host": "www.twse.com.tw",
          "Origin": "https://www.twse.com.tw",
          "Referer": "https://www.twse.com.tw/zh/page/trading/exchange/STOCK_DAY.html",
          # "sec-ch-ua": 'Chromium";v="110", "Not A(Brand";v="24", "Google Chrome";v="110',
          # "sec-ch-ua-mobile": "?0",
          # "sec-ch-ua-platform": '"macOS"',
          # "Sec-Fetch-Dest": "empty",
          # "Sec-Fetch-Mode": "cors",
          # "Sec-Fetch-Site": "same-origin",
          "X-Requested-With": "XMLHttpRequest",
        }
      else:
        url = "https://www.marketwatch.com/investing/stock/{}/download-data".format(stock_name)

      # avg_volume = self.compute_avg_volume(stock_id)
      volumes_list = []
      successful_days = 0
      failed_days = 0
      day_counter = 0
      first_day_success = 0
      blocked = 0
      qualified_days = 0
      html = None
      stock_type_failed = False
      for day in days[::-1]:
        success = 0
        day_counter += 1
        counter = 0
        if stock_name in stocks_data and day in stocks_data[stock_name]:
          volumes_list.append(float(stocks_data[stock_name][day]['volume']))
          successful_days += 1
        else:
          print("{} {}: {}".format('(From cached html)' if html and html != 'connection_error' else '', day, url))
          while counter < self.RETRY:
            counter += 1
            if self.stock_type == 'tw':
              time.sleep(3)

            html = html if html and html != 'connection_error' else self.html_get(url, stock_name=stock_name, verify_item='MW_Masthead_Logo' if self.stock_type != 'tw' else None, header=header)
            if html == 'connection_error':
              connection_error = 1
            elif html != -1:
              connection_error = 0

              success, index_dict, volume = self.get_index_volume_from_html(html, day, stock_website)
              if success is not True:
                if self.stock_type == 'tw' and not stock_type_failed:
                  if stock_name not in self.stock_failed_ignored_tw:
                    self.line_notify('', '', f"!!! ({stock_name} - {stock_id} - {day}) TW Stock Index Url might have changed ({url}) !!!")
                  first = days_last.replace(day=1)
                  last_month = first - timedelta(days=1)
                  url = url_pattern.format(last_month.strftime("%Y%m%d"), stock_name, random.uniform(self.FETCH_TIMEOUT_FROM, self.FETCH_TIMEOUT_TO))
                  counter = 0
                  stock_type_failed = True
                  html = None
                  print("=== Go to the previous month to get the data ===")
                  continue
                counter = self.RETRY
                break

              volumes_list.append(volume)
              if stock_name not in stocks_data:
                stocks_data[stock_name] = { day: {} }
              elif day not in stocks_data[stock_name]:
                stocks_data[stock_name][day] = {}
              stocks_data[stock_name][day]['index'] = index_dict['index']
              stocks_data[stock_name][day]['volume'] = volume

              self.insert_index_volume(day, stock_id, index_dict, volume)
              # ma = {}
              # for ma_days in [5, 10, 20, 60, 50, 150, 200]:
              #     if ma_days not in ma or ma[ma_days] == 0:
              #         ma[ma_days] = stocks_data[stock_name][day]['ma_{}'.format(ma_days)] = self.compute_ma(ma_days, stock_id, day)
              # self.insert_ma(stock_id, day, ma)

              successful_days += 1
              break

        if counter >= self.RETRY:
          print('Not Found: {} => {}'.format(stock_name, day))
          if success == -1:
            self.add_failed_stock(stock_name)

          failed_days += 1
          if failed_days >= 3 and failed_days > successful_days and not connection_error and not first_day_success:
            blocked = 1
            break
        else:
          if day_counter == 1:
            first_day_success = 1

          print("\n==={} {}".format(stock_name, day))
          print(stocks_data[stock_name][day])
          print("---{} {}\n".format(stock_name, day))

      if blocked and not self.test:
        print("Add {} into blocked list".format(stock_name))
        self.add_blocked_stock(stock_name)
        continue

      day_keys = sorted(stocks_data[stock_name].keys()) if stock_name in stocks_data else []
      for day in day_keys:
        ma = {}
        for ma_days in [5, 10, 20, 60, 50, 150, 200]:
          if ma_days not in ma or ma[ma_days] == 0:
            ma[ma_days] = stocks_data[stock_name][day]['ma_{}'.format(ma_days)] = self.compute_ma(ma_days, stock_id, day)
        self.insert_ma(stock_id, day, ma)

        if ((self.stock_type == 'tw' and ipo_delta_days <= 365) or stocks_data[stock_name][day]['volume'] >= self.volume_lowest) \
        and \
        stocks_data[stock_name][day]['index'] >= self.index_lowest \
        and \
        stocks_data[stock_name][day]['index'] <= self.index_highest \
        and \
        stocks_data[stock_name][day]['index'] > stocks_data[stock_name][day]['ma_20'] * self.ratio_ma20 \
        and \
        stocks_data[stock_name][day]['ma_5'] > stocks_data[stock_name][day]['ma_50'] * self.ratio_ma50 \
        and \
        stocks_data[stock_name][day]['ma_10'] > stocks_data[stock_name][day]['ma_50'] * self.ratio_ma50 \
        and \
        stocks_data[stock_name][day]['ma_20'] > stocks_data[stock_name][day]['ma_200'] * self.ratio_ma200:
        # and \
        # (stocks_data[stock_name][day]['ma_50'] >= stocks_data[stock_name][day]['ma_150'] * self.ratio_ma150 or stocks_data[stock_name][day]['ma_50'] >= stocks_data[stock_name][day]['ma_200'] * self.ratio_ma200)
        # and \
        # stocks_data[stock_name][day]['volume'] <= float(avg_volume) * self.volume_ratio
        # and \
        # self.qualified_year_max_min(stock_id, stocks_data[stock_name][day]['index']):
          qualified_days += 1

      rs = 0
      # len(stocks_data[stock_name]) == num_days and days[-1] in stocks_data[stock_name] and days[0] in stocks_data[stock_name]:
      if len(day_keys) >= self.num_days / 2:
        if stocks_data[stock_name][day_keys[0]]['index'] == 0:
          continue
        rs = (stocks_data[stock_name][day_keys[-1]]['index'] - stocks_data[stock_name][day_keys[0]]['index']) / stocks_data[stock_name][day_keys[0]]['index']
        self.rs += rs
        self.rs_counter += 1
        row = self.compute_min_max_avg(stock_name)
        avg_volume = sum(volumes_list[1:]) / (len(day_keys) - 1)

        print(f">>> Days: {self.days[0]} - {self.days[-1]} <<<")
        print('rs: {}'.format(rs))
        print('self.rs: {}'.format(self.rs))
        print('self.rs_counter: {}'.format(self.rs_counter))
        if day_keys[-2] in stocks_data[stock_name]:
          print('yesterday: {}'.format(stocks_data[stock_name][day_keys[-2]]['index']))
        print('first day: {}'.format(stocks_data[stock_name][day_keys[0]]['index']))
        print('qualified_days: {}'.format(qualified_days))
        print(f"number of qualified days must be higher than: {len(day_keys) * self.qualified_days_ratio}")
        if avg_volume > 0:
          # print('volume percent: {}'.format(volumes_list[0] / avg_volume))
          print(f"today's volume: {volumes_list[0]}")
          print(f"volume must be lower than: {float(avg_volume) * self.volume_ratio}")
        print('today: {}'.format(stocks_data[stock_name][day_keys[-1]]['index']))
        print(f"index must between {row['avg'] * (1 - self.stagnate_ratio)} and {row['avg'] * (1 + self.stagnate_ratio)}")

        # volume_base > 0 and volumes_list[0] / volume_base >= 1.45 :
        if stocks_data[stock_name][day_keys[-1]]['volume'] <= float(avg_volume) * self.volume_ratio \
        and \
        self.qualified_year_max_min(stock_id, stocks_data[stock_name][day_keys[-1]]['index']) \
        and \
        stocks_data[stock_name][day_keys[-1]]['index'] >= row['avg'] * (1 - self.stagnate_ratio) and stocks_data[stock_name][day_keys[-1]]['index'] <= row['avg'] * (1 + self.stagnate_ratio) \
        and \
        qualified_days >= len(day_keys) * self.qualified_days_ratio:
          # if 2 <= qualified_days <= 5:
          # gsn = {'stock_name': stock_name, 'rs': rs}
          # all_good_stock_names.append(gsn)

          # if self.is_stagnate_stock(stock_name) and (not self.good(stock_name) or self.test):
          if not self.test:
            self.cursor.execute("INSERT IGNORE INTO good_stock (date, stock_name) VALUES (%s, %s)", (self.today_ymd, stock_name))
            self.conn.commit()
          if self.stock_type != 'tw' or self.able_to_show(stock_name):
            good_stock_names.append(stock_name + (f"({dividend_info})" if dividend_info else ''))
            self.export_file.write(f"{stock_name}\n")
            if len(good_stock_names) >= 10:
              self.line_notify('', good_stock_names)
              good_stock_names = []

      print(f"___ {stock_name} ___")

    if len(good_stock_names):
      self.line_notify('', good_stock_names)

    # return all_good_stock_names
