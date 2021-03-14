from user_agent import generate_user_agent
from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qs, urlencode
from datetime import datetime, timedelta
from collections import defaultdict
import requests
import time
import random
import traceback
import re
import json
import sys, os

class Util:
    RETRY = 3
    REQUEST_TIMEOUT = 6
    FETCH_TIMEOUT_FROM = 1
    FETCH_TIMEOUT_TO = 2
    PROXIES = [ '167.71.5.83:8080', '139.59.1.14:3128', '138.68.60.8:8080', '175.141.69.200:80', '165.227.83.185:3128', '154.16.63.16:3128', '5.252.161.48:3128', '154.16.202.22:3128', '161.35.70.249:8080', '89.187.177.105:80', '191.96.71.118:3128', '89.187.177.99:80', '203.202.245.62:80', '89.187.177.91:80', '89.187.177.92:80', '89.187.177.85:80', '115.243.116.151:80', '136.233.220.215:80', '46.4.96.137:3128', '195.146.50.22:3128', '35.235.115.241:80', '23.105.225.150:80', '88.198.50.103:8080', '51.81.113.246:80', '139.162.78.109:8080', '88.198.24.108:8080', '68.94.189.60:80', '203.202.245.58:80', '176.9.75.42:8080', '78.47.16.54:80', '198.199.86.11:8080', '128.199.202.122:8080', '209.97.150.167:8080', '134.209.29.120:3128', '159.203.61.169:3128', '191.96.42.80:8080', '176.9.119.170:8080', '102.129.249.120:8080' ] # https://free-proxy-list.net/

    def __init__(self):
        pass

    def nested_dict(self):
       return defaultdict(self.nested_dict)

    def print_nested_dict(self, nd):
       import json
       print(json.loads(json.dumps(nd)))

    def get_proxy(self):
        random.seed(time.time())
        offset = random.randint(0, len(self.PROXIES) - 1)
        proxies = {"http": "http://" + self.PROXIES[offset]}

        return proxies

    def update_proxy(self):
        from selenium import webdriver
        from webdriver_manager.chrome import ChromeDriverManager

        self.PROXIES = []
        retry = 0
        while retry < self.RETRY:
            try:
                driver = webdriver.Chrome(ChromeDriverManager().install())
                driver.implicitly_wait(10)
                driver.get('https://free-proxy-list.net/')

                driver.find_element_by_class_name('fa-clipboard').click()
                for line in driver.find_element_by_class_name('modal-body').find_element_by_class_name('form-control').get_attribute('value').split('\n'):
                    line = line.strip()
                    if re.match("\d+\.\d+\.\d+\.\d+:\d+", line, re.I):
                        self.PROXIES.append(line)

                break
            except:
                driver.quit()
                traceback.print_exc()
                retry += 1
                print('Retrying...' + str(retry))

    def html_get(self, url, text=True, use_proxy=True):
        counter = 0
        connection_error = 0
        while counter < self.RETRY:
            counter += 1
            try:
                header = {'User-Agent': generate_user_agent()}
                if use_proxy:
                    proxies = self.get_proxy()
                    print("PROXY => {:}".format(proxies))
                res = requests.get(url, headers=header, proxies=proxies, timeout=self.REQUEST_TIMEOUT)
                break
            except requests.ConnectionError:
                connection_error += 1
            except requests.TooManyRedirects:
                pass
            except:
                traceback.print_exc()
                time.sleep(0.5)

        if counter >= self.RETRY:
            print('Failed retrying!')

            if connection_error >= self.RETRY:
                return 'connection_error'
            return -1
        else:
            if text:
                return res.text
            else:
                return res.content

    def to_ymd(self, dt, sep='/'):
        dt = dt.split(sep)
        return "{}-{}-{}".format(dt[2], dt[0], dt[1])


class MysqlUtil(Util):
    def __init__(self):
        Util.__init__(self)

    def create_sql_conn(self):
        import pymysql

        try:
            with open('cred.json') as json_file:
                cred = json.load(json_file)
            conn = pymysql.connect(host=cred['mysql']['ip'], port=cred['mysql']['port'], user=cred['mysql']['username'], passwd=cred['mysql']['password'], db=cred['mysql']['database'], charset="utf8")
            cursor = conn.cursor(pymysql.cursors.DictCursor)

            return (conn, cursor)
        except:
            sys.exit("Error: unable to create database connection.")

    def close_sql_conn(self, conn, cursor):
        if cursor:
            cursor.close()
        if conn:
            conn.close()


class StockUtil(Util):
    insert_index_volume_sql = 'INSERT IGNORE INTO index_volume (`date`, stock_id, `index`, volume) VALUES(%s, %s, %s, %s)'
    qualified_days_ratio = 0.7

    def __init__(self, conn=None, cursor=None):
        Util.__init__(self)

        if conn and cursor:
            (self.conn, self.cursor) = (conn, cursor)
        else:
            mysql_util = MysqlUtil()
            (self.conn, self.cursor) = mysql_util.create_sql_conn()

        for d in ['GICS', 'GICS/finished']:
            if not os.path.exists(d):
                os.makedirs(d)

    def transform_m_b_to_number(self, num):
        ch_to_num = {
            'k': 1000,
            'm': 1000000,
            'b': 1000000000,
        }

        sign = 1
        if '(' in num:
            sign = -1
        num = num.replace('(', '').replace(')', '').lower()

        for ch in ch_to_num.keys():
            if ch in num:
                return float(num.replace(ch, ''))*ch_to_num[ch]*sign

        if not num.isdigit():
            return False

        return float(num)*sign

    def get_stock_id(self, stock_name):
        select_stock_id_sql     = 'SELECT id FROM stock_list WHERE stock_name = %s'
        insert_stock_name_sql   = 'INSERT INTO stock_list (stock_name) VALUES(%s)'

        self.cursor.execute(select_stock_id_sql, (stock_name))
        stock_id = self.cursor.fetchone()
        if stock_id and 'id' in stock_id:
            stock_id = stock_id['id']
        else:
            self.cursor.execute(insert_stock_name_sql, (stock_name))
            self.conn.commit()
            stock_id = self.cursor.lastrowid

        return stock_id

    def get_stock_info(self, stock_name, use_proxy=True):
        from hanziconv import HanziConv

        headers = {
            'X-Requested-With': 'XMLHttpRequest',
            'Referer': 'http://xueqiu.com/p/ZH010389',
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.2; WOW64; rv:38.0) Gecko/20100101 Firefox/38.0',
            'Host': 'xueqiu.com',
            #'Connection':'keep-alive',
            #'Accept':'*/*',
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

    def insert_index_volume(self, date, stock_id, index, volume):
        self.cursor.execute(self.insert_index_volume_sql, (date, stock_id, index, volume))
        self.conn.commit()

    def get_index_volume(self, path):
        # cursor.execute('SELECT stock_name FROM stock_list')
        # all_stock_names = [ stock_name['stock_name'] for stock_name in cursor.fetchall() ]

        url = 'https://www.marketwatch.com/investing/stock/{0}/download-data'

        import csv
        import pandas as pd
        from io import StringIO

        for file_name in os.listdir(path):
            if file_name.endswith(('test' if self.test else '') + ".csv"):
                print("===Processing {}===".format(file_name))

                with open(path + '/' + file_name, encoding="utf8", newline='') as csvfile:
                    rows = csv.reader(csvfile, delimiter=',')
                    headers = next(rows, None)
                    for row in rows:
                        for idx, col in enumerate(row, start=0):
                            if not col or '/' not in col:
                                continue

                            stock_name = col.split('/')[-1]
                            # if stock_name in all_stock_names:
                            #     continue
                            print("Processing {}".format(stock_name))

                            download_link = None
                            html = self.html_get(url.format(stock_name))
                            if html != -1:
                                soup = BeautifulSoup(html, 'html.parser')
                                for a_href in soup.findAll("a"):
                                    if a_href.text == 'Download Data (.csv)':
                                        download_link = a_href['href']
                                        break

                            if not download_link:
                                continue

                            params = parse_qs(urlparse(download_link).query)
                            params['startdate'][0] = (datetime.now() - timedelta(days=365)).strftime("%m/%d/%Y 00:00:00")
                            params['enddate'][0] = datetime.now().strftime("%m/%d/%Y 23:59:59")
                            download_link = "{}?{}".format(download_link[:download_link.find('?')], urlencode(params, doseq=True))
                            print("download_link: {}".format(download_link))

                            stock_id = self.get_stock_id(stock_name)
                            print("stock_id: {}".format(stock_id))

                            try:
                                content = self.html_get(download_link)
                                df = pd.read_csv(StringIO(content))
                                rows = []
                                for _, row in df.iterrows():
                                    dt = self.to_ymd(row['Date'])
                                    idx = float(str(row['Close']).replace('$', '').replace(',', ''))
                                    volume = self.transform_m_b_to_number(row['Volume'].replace(',', ''))
                                    rows.append((dt, stock_id, idx, volume))
                                print(rows)
                                self.cursor.executemany(self.insert_index_volume_sql, rows)
                                self.conn.commit()
                            except:
                                traceback.print_exc()

                print("---Finished {}---".format(file_name))

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
                for ele in soup.find_all('i', {'class' : 'list-style'}):
                    if re.search("GICS行业", ele.parent.text):
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

    def renew_categories_index_volume(self):
        self.get_Xueqiu_categories()
        self.get_index_volume('.' if self.test else 'GICS')
        self.retrospect_ma(365)

    def line_notify(self, group=None, good_stock_names=None):
        from datetime import date

        with open('cred.json') as json_file:
            cred = json.load(json_file)
            token = cred['line']['token']

        headers = {
            "Authorization": "Bearer {}".format(token),
            "Content-Type": "application/x-www-form-urlencoded"
        }
        params = { "message": "[{}] {}".format(group, ', '.join(good_stock_names)) if group else "=== {} ===".format(date.today()) }

        try:
            r = requests.post("https://notify-api.line.me/api/notify", headers=headers, params=params)
        except:
            print("Line notification failed.")
            traceback.print_exc()

    def get_stock_days(self, num_days=20):
        skip_dates = [ '2020-11-26', '2020-12-25', '2021-01-01', '2021-01-18', '2021-02-15' ]

        import pandas as pd
        from pandas.tseries.offsets import BDay
        if not self.test:
            today = pd.datetime.today()
        else:
            today = pd.to_datetime(input("Enter date: ")) #pd.to_datetime('2020-10-29')
        days = []
        shift = 0
        while True:
            nds = num_days + shift
            for delta in range(nds, 0, -1):
                d = datetime.strftime(today - BDay(delta), '%Y-%m-%d')
                if d not in skip_dates:
                    days.append(d)

            if len(days) != num_days:
                shift += 1
                days = []
            else:
                break

        return days

    def get_blocked_stocks(self):
        select_blocked_sql = '''
            SELECT      stock_name
            FROM        blocked_stock bs
            INNER JOIN  stock_list sl ON sl.id = bs.stock_id
        '''

        blocked_stocks = []
        self.cursor.execute(select_blocked_sql)
        for row in self.cursor.fetchall():
            blocked_stocks.append(row['stock_name'])

        return blocked_stocks

    def add_blocked_stock(self, stock_id):
        insert_blocked_sql = 'INSERT INTO blocked_stock (stock_id, update_time) VALUES (%s, NOW()) ON DUPLICATE KEY UPDATE update_time = NOW()'
        self.cursor.execute(insert_blocked_sql, (stock_id))
        self.conn.commit()

    def compute_ma(self, ma, stock_id, date):
        select_sql_ma = 'SELECT COUNT(*) days, SUM(`index`) `sum` FROM (SELECT `index` FROM index_volume WHERE stock_id = %s AND date <= %s ORDER BY `date` DESC LIMIT %s) AS ma'

        self.cursor.execute(select_sql_ma, (stock_id, date, ma))
        row = self.cursor.fetchone()
        return row['sum'] / ma if row['days'] == ma else 0

    def insert_ma(self, stock_id, date, ma=None):
        if not ma:
            ma = {}
        for ma_days in [50, 150, 200]:
            if ma_days not in ma:
                ma[ma_days] = self.compute_ma(ma_days, stock_id, date)

        update_ma_sql = 'UPDATE index_volume SET ma_50 = %s, ma_150 = %s, ma_200 = %s WHERE stock_id = %s AND date = %s'
        self.cursor.execute(update_ma_sql, (ma[50], ma[150], ma[200], stock_id, date))
        self.conn.commit()

    def retrospect_ma(self, stock_id=None, days=None):
        if not days:
            days = self.get_stock_days()
        elif isinstance(days, int):
            days = self.get_stock_days(days)

        zero_ma_date = self.retrieve_zero_ma_date()

        stock_id_sql = 'WHERE stock_id = {}'.format(self.conn.escape(stock_id)) if stock_id else ''
        self.cursor.execute('''
            SELECT DISTINCT(stock_id), sl.stock_name
            FROM index_volume iv
            INNER JOIN stock_list sl ON sl.`id` = iv.`stock_id`
            {}
        '''.format(stock_id_sql))
        for row in self.cursor.fetchall():
            stock_id = row['stock_id']
            print('=== Computing ma: {}({}) ==='.format(row['stock_name'], stock_id))
            for dt in list(filter(lambda d: d <= zero_ma_date[stock_id], days)):
                self.insert_ma(stock_id, dt)

    def retrieve_zero_ma_date(self, stock_id=None):
        self.cursor.execute('''
            SELECT stock_id, MAX(`date`) zero_date
            FROM index_volume
            WHERE ma_50 = 0 AND ma_150 = 0 AND ma_200 = 0
            {}
            GROUP BY stock_id
        '''.format('AND stock_id = ' . self.conn.escape(stock_id) if stock_id else ''))

        return { row['stock_id']: row['zero_date'] for row in self.cursor.fetchall() }

    def compute_avg_volume(self, stock_id=None, days=50):
        self.cursor.execute('''
            SELECT AVG(volume) avg_volume
            FROM (
                SELECT volume
                FROM index_volume
                WHERE stock_id = %s
                ORDER BY date DESC
                LIMIT %s
            ) t
        ''', (stock_id, days))
        row = self.cursor.fetchone()

        return row['avg_volume'] if row['avg_volume'] else 0

    def get_one_year_max_min_index(self, stock_id):
        self.cursor.execute('''
            SELECT MAX(`index`) max_index, MIN(`index`) min_index
            FROM (
                SELECT `index`
                FROM index_volume
                WHERE stock_id = %s
                ORDER BY `date` DESC
                LIMIT 250
            ) t
        ''', (stock_id))
        row = self.cursor.fetchone()

        return (row['max_index'], row['min_index'])

    def qualified_year_max_min(self, stock_id, index):
        year_max, year_min = self.get_one_year_max_min_index(stock_id)
        # return abs(index - year_max) / year_max <= 0.25 and (index - year_min) / year_min >= 0.3
        return (index - year_min) / year_min >= 0.3

    def get_index_volume_from_html(self, html, day):
        soup = BeautifulSoup(html, 'html.parser')
        counter = 1
        while True:
            try:
                tr = soup.find("div", {"class": "download-data"}).select('tr')[counter]
                if self.to_ymd(tr.select('td')[0].select('div')[0].text, '/') < day:
                    counter += 1
                    continue
                else:
                    return False, None, None
                index = tr.select('td')[4].text.replace('$', '').replace(',', '')
                index = float(index)
                volume = tr.select('td')[5].text.replace(',', '')
                volume = self.transform_m_b_to_number(volume)

                return True, index, volume
            except:
                traceback.print_exc()
                time.sleep(1)
                return False, None, None

    def get_stock_daily(self, file_name):
        num_days = 20
        days = self.get_stock_days(num_days)
        blocked_stocks = self.get_blocked_stocks()
        good_stock_names = []
        self.rs = 0
        self.rs_counter = 0

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
                    stock_list.append(stock_name)

            for stock_name in stock_list:
                if stock_name in blocked_stocks:
                    continue

                print('***' + stock_name + '***')
                stock_id = self.get_stock_id(stock_name)

                self.cursor.execute("SELECT `date`, `index`, volume, ma_50, ma_150, ma_200 FROM index_volume WHERE stock_id = %s AND `date` >= %s AND `date` <= %s", (stock_id, days[0], days[-1]))
                stocks_data = {}
                for row in self.cursor.fetchall():
                    if stock_name not in stocks_data:
                        stocks_data[stock_name] = { row['date']: {} }
                    elif row['date'] not in stocks_data[stock_name]:
                        stocks_data[stock_name][row['date']] = {}

                    for key in ['index', 'volume', 'ma_50', 'ma_150', 'ma_200']:
                        stocks_data[stock_name][row['date']][key] = row[key]

                url = "https://www.marketwatch.com/investing/stock/" + stock_name + "/download-data"
                avg_volume = self.compute_avg_volume(stock_id)
                volumes_list = []
                successful_days = 0
                failed_days = 0
                day_counter = 0
                first_day_success = 0
                blocked = 0
                qualified_days = 0
                for day in days[::-1]:
                    day_counter += 1
                    counter = 0
                    if stock_name in stocks_data and day in stocks_data[stock_name]:
                        volumes_list.append(float(stocks_data[stock_name][day]['volume']))
                        successful_days += 1
                    else:
                        print("{}: {}".format(day, url))
                        while counter < self.RETRY:
                            counter += 1

                            html = self.html_get(url)
                            if html == 'connection_error':
                                connection_error = 1
                            elif html != -1:
                                connection_error = 0

                                success, index, volume = self.get_index_volume_from_html(html, day)
                                if not success:
                                    counter = self.RETRY
                                    break

                                volumes_list.append(volume)
                                if stock_name not in stocks_data:
                                    stocks_data[stock_name] = { day: {} }
                                elif day not in stocks_data[stock_name]:
                                    stocks_data[stock_name][day] = {}
                                stocks_data[stock_name][day]['index'] = index
                                stocks_data[stock_name][day]['volume'] = volume

                                self.insert_index_volume(day, stock_id, index, volume)
                                ma = {}
                                for ma_days in [50, 150, 200]:
                                    if ma_days not in ma:
                                        ma[ma_days] = stocks_data[stock_name][day]['ma_{}'.format(ma_days)] = self.compute_ma(ma_days, stock_id, day)
                                self.insert_ma(stock_id, day, ma)

                                successful_days += 1
                                break

                    if counter >= self.RETRY:
                        print('Not Found: {} => {}'.format(stock_name, day))

                        failed_days += 1
                        if failed_days >= 3 and failed_days > successful_days and not connection_error and not first_day_success:
                            blocked = 1
                            break
                    else:
                        if day_counter == 1:
                            first_day_success = 1

                        print('==={} {}'.format(stock_name, day))
                        print(stocks_data[stock_name][day])
                        print(avg_volume)
                        print(stocks_data[stock_name][day]['volume'] <= avg_volume)
                        print(self.qualified_year_max_min(stock_id, stocks_data[stock_name][day]['index']))
                        print(stocks_data[stock_name][day]['index'] >= stocks_data[stock_name][day]['ma_50'])
                        print(stocks_data[stock_name][day]['ma_50'] >= stocks_data[stock_name][day]['ma_150'])
                        print(stocks_data[stock_name][day]['ma_50'] >= stocks_data[stock_name][day]['ma_200'])
                        print('---{} {}'.format(stock_name, day))

                        if stocks_data[stock_name][day]['volume'] <= avg_volume and self.qualified_year_max_min(stock_id, stocks_data[stock_name][day]['index']) and stocks_data[stock_name][day]['index'] >= stocks_data[stock_name][day]['ma_50'] and (stocks_data[stock_name][day]['ma_50'] >= stocks_data[stock_name][day]['ma_150'] or stocks_data[stock_name][day]['ma_50'] >= stocks_data[stock_name][day]['ma_200']):
                            qualified_days += 1

                if blocked:
                    print("Add {} into blocked list".format(stock_name))
                    self.add_blocked_stock(stock_id)

                volume_base = sum(volumes_list[1:]) / (len(days) - 1)
                print(volumes_list)
                print(volume_base)
                if volume_base > 0:
                    print('percent: {}'.format(volumes_list[0] / volume_base))

                rs = 0
                if stock_name in stocks_data and len(stocks_data[stock_name]) == num_days and days[-1] in stocks_data[stock_name] and days[0] in stocks_data[stock_name]:
                    rs = (stocks_data[stock_name][days[-1]]['index'] - stocks_data[stock_name][days[0]]['index']) / stocks_data[stock_name][days[0]]['index']
                    self.rs += rs
                    self.rs_counter += 1

                    print('rs: {}'.format(rs))
                    print('self.rs: {}'.format(self.rs))
                    print('self.rs_counter: {}'.format(self.rs_counter))
                    print('today: {}'.format(stocks_data[stock_name][days[-1]]['index']))
                    if days[-2] in stocks_data[stock_name]:
                        print('yesterday: {}'.format(stocks_data[stock_name][days[-2]]['index']))
                    print('first day: {}'.format(stocks_data[stock_name][days[0]]['index']))
                    print('qualified_days: {}'.format(qualified_days))

                    if stocks_data[stock_name][days[-1]]['volume'] <= avg_volume and qualified_days >= len(days) * self.qualified_days_ratio: #and volume_base > 0 and volumes_list[0] / volume_base >= 1.45 :
                        good_stock_names.append({'stock_name': stock_name, 'rs': rs})

                print('---' + stock_name + '---')

        return good_stock_names