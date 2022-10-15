import webbrowser
import traceback
import requests
from user_agent import generate_user_agent
import time
from util import StockUtil, MysqlUtil

RETRY = 2
REQUEST_TIMEOUT = 20

def get_html(url, use_proxy=False):
    counter = 0
    while counter < RETRY:
        counter += 1
        try:
            header = {'User-Agent': generate_user_agent()}
            proxies = {}
            res = requests.get(url, headers=header, proxies=proxies, timeout=REQUEST_TIMEOUT)
            break
        except:
            traceback.print_exc()
            time.sleep(0.5)

    if counter >= RETRY:
        print('Failed retrying!')
        return -1
    else:
        return res.text

def extract_stock_holders(stock_name):
    for exchange in ['xnys', 'xnas']:
        url = 'https://www.morningstar.com/stocks/' + exchange + '/' + stock_name + '/ownership'
        res = get_html(url)
        if 'Page Not Found' in res:
            continue
        return url

def insert_manually_blocked_stocks():
    sql = []
    file = open("manual.txt", "r")
    for stock in file.read().splitlines():
        sql.append("('{}', NOW(), 'manual')".format(stock.upper()))

    mysql_util = MysqlUtil()
    (conn, cursor) = mysql_util.create_sql_conn()
    insert_stock_sql = "INSERT INTO `blocked_stock` (`stock_name`, `update_time`, `type`) VALUES\n" + ", \n".join(sql) + ';'
    cursor.execute(insert_stock_sql)
    conn.commit()

if __name__ == "__main__":
    insert_manually_blocked_stocks()

    while False:
        val = input("Enter stock name: ")
        # webbrowser.open('https://seekingalpha.com/symbol/' + val + '/transcripts')
        # webbrowser.open('https://finance.yahoo.com/quote/' + val)
        # webbrowser.open('https://www.dataroma.com/m/ins/ins.php?t=q&am=0&sym=' + val + '&o=fd&d=d')
        # webbrowser.open('https://www.nasdaq.com/market-activity/stocks/' + val + '/institutional-holdings')
        # webbrowser.open(extract_stock_holders(val))

        webbrowser.open('https://xueqiu.com/S/' + val)
        webbrowser.open('https://www.marketwatch.com/investing/stock/' + val + '/financials/income/quarter')
        webbrowser.open('https://www.marketwatch.com/investing/stock/' + val + '/financials/cash-flow/quarter')
        # webbrowser.open('https://www.mg21.com/' + val + '.html')
        webbrowser.open('https://seekingalpha.com/symbol/' + val)
        # webbrowser.open('https://www.nasdaq.com/market-activity/stocks/' + val + '/insider-activity')
        # webbrowser.open('https://www.nasdaq.com/market-activity/stocks/' + val + '/institutional-holdings')
        # try:
        #     webbrowser.open('https://www.gurufocus.com/stock/' + val + '/ownership')
        # except:
        #    traceback.print_exc()
