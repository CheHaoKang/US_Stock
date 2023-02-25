import os
from util import StockUtil, MysqlUtil
import shutil
import sys
import getopt
import datetime

def move_directory(source, destination):
    for file_name in os.listdir(source):
        if file_name.endswith(".csv"):
            shutil.move(os.path.join(source, file_name), destination)

if __name__ == "__main__":
    file_name = None
    stock_website = None
    proxy_page = None
    try:
      opts, args = getopt.getopt(sys.argv[1:], "f:s:p:", [ "file=", "stock_website=", "proxy_page=" ])
    except getopt.GetoptError:
        print(sys.argv[0] + ' --file <file> --stock_website <stock_website> --proxy_page <proxy_page>')
        sys.exit(0)
    for opt, arg in opts:
        if opt in ("-f", "--file"):
            file_name = arg
        elif opt in ("-s", "--stock_website"):
            stock_website = arg
        elif opt in ("-p", "--proxy_page"):
            proxy_page = int(arg)

    today = datetime.date.today()
    export_file = open(f"list_{today}-{file_name}.txt", "w")
    stock_util = StockUtil(export_file)
    mysql_util = MysqlUtil(stock_util.conn, stock_util.cursor)

    stock_util.test = 0 # Change date of today = pd.to_datetime('2020-11-10'), modify stocks-Xueqiu-test.csv
    stock_util.get_new_stocks = 0
    stock_util.stagnate = 0
    # stock_util.stagnate = 1 if len(sys.argv) >= 2 and sys.argv[1] == '--stagnate' else 0

    if not (stock_util.test or stock_util.stagnate):
        stock_util.update_proxy(proxy_page)

    if stock_util.get_new_stocks:
        # if os.path.exists('GICS/backup'):
        #     shutil.rmtree('GICS/backup')
        # os.makedirs('GICS/backup')
        # move_directory('GICS', 'GICS/backup')
        stock_util.renew_index_volume(file_name)
        # shutil.rmtree('GICS/finished')
        sys.exit(0)

    # if interrupted, comment out this block
    # if not stock_util.test:
    #     move_directory('GICS/finished', 'GICS')

    folder = '.' if stock_util.test else 'GICS'
    if True:
        if not stock_util.get_stock_days():
            print('Stock market closed yesterday.')
            sys.exit(0)

        stock_util.insert_manually_blocked_stocks()
        stock_util.line_notify()
        for filename in os.listdir(folder):
            if not filename.endswith(('test' if stock_util.test else '') + ".csv"):
                continue
            if file_name and file_name != filename:
                    continue

            print("=== Processing {} ===".format(filename))
            group = filename.split('-')[2].replace('.csv', '')
            if stock_util.stagnate:
                stagnating_stocks = stock_util.find_stagnate_stocks('{}/{}'.format(folder, filename))
                if stagnating_stocks:
                    print('>>> Stagnating')
                    print("\n".join(stagnating_stocks))
            else:
                # stagnating_stocks = stock_util.find_stagnate_stocks('{}/{}'.format(folder, filename))
                stagnating_stocks = []
                good_stock_names = stock_util.get_stock_daily('{}/{}'.format(folder, filename), stock_website)
                # continue
                print('rs: {}'.format(stock_util.rs))
                print('rs_counter: {}'.format(stock_util.rs_counter))
                if stock_util.rs_counter:
                    rs_all = stock_util.rs / stock_util.rs_counter
                    print('rs / rs_counter: {}'.format(rs_all))
                    print('rs / rs_counter * {}: {}'.format(1 + stock_util.rs_ratio, rs_all * (1 + stock_util.rs_ratio)))
                # if good_stock_names:
                if False:
                    gsn = []
                    for one in good_stock_names:
                        starred = ''
                        if stagnating_stocks and one in stagnating_stocks:
                            starred = '*'

                        print("{}, {}, stagnate: {}".format(one, rs_all * (1 + stock_util.rs_ratio), stock_util.is_stagnate_stock(one['stock_name'])))

                        caught, line_str = False, ''
                        if rs_all <= 0:
                            if one['rs'] >= rs_all + abs(rs_all) * stock_util.rs_ratio:
                                caught = True
                                line_str = '{}{}({})'.format(starred, one['stock_name'], '+')
                        elif one['rs'] >= rs_all * (1 + stock_util.rs_ratio):
                            caught = True
                            line_str = '{}{}({})'.format(starred, one['stock_name'], round(one['rs'] / rs_all, 2))

                        if caught:
                            if not stock_util.test:
                                mysql_util.execute_sql("INSERT IGNORE INTO good_stock (date, stock_name, strength) VALUES (%s, %s, %s)", (stock_util.today.strftime("%Y-%m-%d"), one['stock_name'], one['rs']))

                            if stock_util.is_stagnate_stock(one['stock_name']) and (not stock_util.good(one['stock_name']) or stock_util.test):
                                gsn.append(line_str)
                                if len(gsn) >= 10:
                                    stock_util.line_notify(group, gsn)
                                    gsn = []
                    if len(gsn):
                        stock_util.line_notify(group, gsn)
                # if not stock_util.test:
                #     os.rename('GICS/' + filename, 'GICS/finished/' + filename)
            stock_util.line_notify('', '', "--- {} ---".format(filename))
            print("--- Finished {} ---".format(filename))

    export_file.close()
    with open('list_all.txt','wb') as wfd:
        for f in [ f"list_{today}-stocks-Xueqiu-all-1.csv.txt", f"list_{today}-stocks-Xueqiu-all-2.csv.txt" ]:
            if not os.path.exists(f):
                continue
            with open(f,'rb') as fd:
                shutil.copyfileobj(fd, wfd)
    shutil.copyfile('list_all.txt', '/home/decken/Dropbox2/Dropbox/list_all.txt')

    # os.system("/usr/bin/mysqldump -u root -p00000000 stocks > /home/decken/Dropbox/Stocks/stocks.sql")
    # time.sleep(1800)

    # import datetime
    # today = datetime.date.today()
    # tomorrow = today + datetime.timedelta(1)
    # os.system('sudo rtcwake -v -t `date -d "' + str(tomorrow) + ' 07:50" +%s` -m mem')


# stock_util.line_notify('1', ['2'])

# opts = Options()
# opts.add_argument('--proxy-server={}'.format(self.get_proxy()['http']))
# opts.add_argument('user-agent=Mozilla/5.0 (Windows NT 6.2; WOW64; rv:38.0) Gecko/20100101 Firefox/38.0')
# opts.add_argument('Referer=http://xueqiu.com/p/ZH010389')
# opts.add_argument('Host=xueqiu.com')
# opts.add_argument('X-Requested-With=XMLHttpRequest')
# opts.add_argument('cookies=s=iabht2os.1dgjn9z; xq_a_token=25916c3bfec27272745f6070d664a48d4b10d322; xq_r_token=2242d232b1aa6ffb6d9569d53e067311db16c12c; __utma=1.19055154.1664983189.1664983189.1665293171.2;__utmc=1; __utmz=1.1664983189.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); Hm_lvt_1db88642e346389874251b5a1eded6e3=1664272385; Hm_lpvt_1db88642e346389874251b5a1eded6e3=1665295164')

# { 'Referer': 'https://stackoverflow.com/', 'Host': 'stackoverflow.com' },
# { 'Referer': 'https://www.youtube.com/', 'Host': 'www.youtube.com' },
# { 'Referer': 'https://tw.dictionary.search.yahoo.com/', 'Host': 'tw.dictionary.search.yahoo.com' },
# { 'Referer': 'https://blog.csdn.net/', 'Host': 'blog.csdn.net' },
# { 'Referer': 'https://dictionary.cambridge.org/', 'Host': 'dictionary.cambridge.org' },
# { 'Referer': 'https://cart.books.com.tw/', 'Host': 'cart.books.com.tw' },
# { 'Referer': 'https://goodinfo.tw/', 'Host': 'goodinfo.tw' },
# { 'Referer': 'https://totality-of-life.com/', 'Host': 'totality-of-life.com' },
# { 'Referer': 'https://jsfiddle.net/', 'Host': 'jsfiddle.net' },
# { 'Referer': 'https://hk.investing.com/', 'Host': 'hk.investing.com' }

# html = self.html_get('https://free-proxy-list.net/', proxy={ "http": "http://{}".format(line) })
# if 'Free Proxy List' in html:
#     print("Proxy \"{}\" is good.".format(line))
#     self.PROXIES.append(line)
# else:
#     print("*** Proxy \"{}\" is BAD.".format(line))

# stock_util.update_proxy()
# from bs4 import BeautifulSoup
# html = stock_util.html_get("https://www.nasdaq.com/market-activity/stocks/vcxau/historical")
# print(html)
# soup = BeautifulSoup(html, 'html.parser')
# tr = soup.find("tbody", {"class": "historical-data__table-body"}).select('tr')[0]
# print(tr)
# print(tr.select('th')[0].text)

# page_day = stock_util.mdy_to_ymd(tr.select('th')[0].text)

# index_open, index_high, index_low, index = map(lambda i: float(tr.select('td')[i].select('span')[0].text.replace('$', '').replace(',', '')), [3, 4, 5, 1])
# volume = tr.select('td')[2].text.replace(',', '')
# volume = stock_util.transform_m_b_to_number(volume)

# print([ page_day, index_open, index_high, index_low, index, volume ])

# exit(0)

# import requests
# res = requests.get('https://www.marketwatch.com/investing/stock/mtvc/download-data')
# print(res.headers)
# print(len(res.text))
# print(len(res.content))
# print(res.raw)
# exit(0)

# file = open("list.txt", "r")
# stocks = {}
# a = file.read()
# for stock in a.split(','):
#     stock = stock.strip("'")
#     stocks[stock] = 1
# for s in sorted(stocks.keys()):
#     print(s)
# exit(0)
