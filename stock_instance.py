import glob
import os
from util import StockUtil, MysqlUtil
import shutil
import sys
import getopt
import datetime
from time import time

def move_directory(source, destination):
    for file_name in os.listdir(source):
        if file_name.endswith(".csv"):
            shutil.move(os.path.join(source, file_name), destination)

if __name__ == "__main__":
    order = None
    stock_website = None
    proxy_page = None
    try:
      opts, args = getopt.getopt(sys.argv[1:], "o:s:p:", [ "order=", "stock_website=", "proxy_page=" ])
    except getopt.GetoptError:
        print(sys.argv[0] + ' --order <order> --stock_website <stock_website> --proxy_page <proxy_page>')
        sys.exit(0)
    for opt, arg in opts:
        if opt in ("-o", "--order"):
            order = int(arg)
        elif opt in ("-s", "--stock_website"):
            stock_website = arg
        elif opt in ("-p", "--proxy_page"):
            proxy_page = int(arg)

    today = datetime.date.today()
    export_file = open(f"list_{today}_{int(time())}.txt", "w")
    stock_util = StockUtil(export_file)

    stock_util.test = 0 # Change date of today = pd.to_datetime('2020-11-10'), modify stocks-Xueqiu-test.csv
    stock_util.get_new_stocks = 0
    stock_util.stagnate = 0

    if not (stock_util.test or stock_util.stagnate):
        stock_util.update_proxy(proxy_page)

    if stock_util.get_new_stocks:
        stock_util.renew_index_volume()
        sys.exit(0)

    if not stock_util.get_stock_days():
        print('Stock market closed yesterday.')
        sys.exit(0)

    print("=== Processing {} ===".format(order))

    stock_util.insert_manually_blocked_stocks()
    stock_util.line_notify()
    stock_util.cursor.execute('''
        SELECT  COUNT(*) AS count
        FROM    stock_list
    ''')
    count = stock_util.cursor.fetchone()['count']
    order_number = 2
    order_size = int(count / order_number)
    limit = count if order == order_number else order_size
    offset = order_size * (order - 1)
    stock_util.cursor.execute(f'''
        SELECT      stock_name
        FROM        stock_list
        ORDER BY    id
        LIMIT {limit} OFFSET {offset}
    ''')
    print(f'''
        === stock sql ===
        SELECT      stock_name
        FROM        stock_list
        ORDER BY    id
        LIMIT {limit} OFFSET {offset}
        --- stock sql ---
    ''')
    good_stock_names = stock_util.get_stock_daily([ stock_name['stock_name'] for stock_name in stock_util.cursor.fetchall() ], stock_website)

    stock_util.line_notify('', '', "--- {} ---".format(order))
    print("--- Finished {} ---".format(order))

    export_file.close()
    with open('list_all.txt','wb') as wfd:
        for f in glob.glob(f"list_{today}*"): #[ f"list_{today}-stocks-Xueqiu-all-1.csv.txt", f"list_{today}-stocks-Xueqiu-all-2.csv.txt" ]:
            # if not os.path.exists(f):
            #     continue
            with open(f,'rb') as fd:
                shutil.copyfileobj(fd, wfd)
    shutil.copyfile('list_all.txt', '/home/decken/Dropbox2/Dropbox/list_all.txt')



    # stock_util = StockUtil()
    # stock_util.get_index_volume_20230225()