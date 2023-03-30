import glob
import os, re
from util import StockUtil, MysqlUtil
import shutil
import sys
import getopt
import datetime
from time import time

def remove_old_files():
  today = datetime.datetime.strftime(datetime.datetime.now() - datetime.timedelta(days = 3), '%Y-%m-%d')
  for f in os.listdir('.'):
    if m := re.search(r'^list.*(\d{4}-\d{2}-\d{2})_\d+.txt$', f):
      if m.group(1) < today:
        os.remove(f)


if __name__ == "__main__":
  # stock_util = StockUtil()
  # stock_util.test = 0
  # # stock_util.get_index_volume_20230225()
  # stock_util.retrospect_ma(stock_id=None, days=stock_util.retrospect_days)
  # exit(0)

  order = 1
  stock_website = None
  proxy_page = 1
  stock_type = ''
  try:
    opts, args = getopt.getopt(sys.argv[1:], "o:s:p:t", [ "order=", "stock_website=", "proxy_page=", "type=" ])
  except getopt.GetoptError:
    print(sys.argv[0] + ' --order <order> --stock_website <stock_website> --proxy_page <proxy_page> --type <type>')
    sys.exit(0)
  for opt, arg in opts:
    if opt in ("-o", "--order"):
      order = int(arg)
    elif opt in ("-s", "--stock_website"):
      stock_website = arg
    elif opt in ("-p", "--proxy_page"):
      proxy_page = int(arg)
    elif opt in ("-t", "--type"):
      stock_type = arg

  remove_old_files()
  today = datetime.date.today()
  export_file = open(f"list_{stock_type}_{today}_{int(time())}.txt", "w")
  stock_util = StockUtil(export_file, stock_type)

  stock_util.test = 0 # Change date of today = pd.to_datetime('2020-11-10'), modify stocks-Xueqiu-test.csv
  stock_util.get_new_stocks = 0
  stock_util.stagnate = 0

  # if not (stock_util.test or stock_util.stagnate):
  #   stock_util.update_proxy(proxy_page)

  if stock_util.get_new_stocks:
    if stock_type == 'tw':
      stock_util.renew_index_volume_tw()
    else:
      stock_util.renew_index_volume()
    sys.exit(0)

  if not stock_util.get_stock_days():
    print('Stock market closed yesterday.')
    sys.exit(0)

  print("=== Processing {} ===".format(order))

  stock_util.insert_manually_blocked_stocks()
  stock_util.line_notify()
  stock_util.line_notify('', '', "=== {} ===".format(order))
  stock_util.cursor.execute('''
    SELECT  COUNT(*) AS count
    FROM    stock_list
  ''')
  count = stock_util.cursor.fetchone()['count']
  # order_size = int(count / (1 if stock_util.stock_type == 'tw' else stock_util.order_number))
  order_size = int(count / 1)
  # limit = count if order == stock_util.order_number else order_size
  limit = order_size
  # offset = order_size * (order - 1)
  offset = 0
  stock_util.cursor.execute(f'''
    SELECT      stock_name
    FROM        stock_list
    # WHERE       stock_name = 6806
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
  stock_util.get_stock_daily([ stock_name['stock_name'] for stock_name in stock_util.cursor.fetchall() ], stock_website)

  stock_util.line_notify('', '', "--- {} ---".format(order))
  print("--- Finished {} ---".format(order))

  export_file.close()
  with open(f"list_{stock_type}_all.txt",'wb') as wfd:
    for f in glob.glob(f"list_{stock_type}_{today}*"): #[ f"list_{today}-stocks-Xueqiu-all-1.csv.txt", f"list_{today}-stocks-Xueqiu-all-2.csv.txt" ]:
      # if not os.path.exists(f):
      #     continue
      with open(f,'rb') as fd:
        shutil.copyfileobj(fd, wfd)
  lines = open(f"list_{stock_type}_all.txt", 'r').readlines()
  index = 1
  FILE_SIZE = 600
  while lines:
    file_name = f"list_{stock_type}_all_{index}.txt"
    with open(file_name, "w") as wfd:
      wfd.write("".join(lines[0:FILE_SIZE]))
    shutil.copyfile(file_name, f"/home/decken/Dropbox2/Dropbox/{file_name}")
    del lines[0:FILE_SIZE]
    index += 1
  shutil.copyfile(f"list_{stock_type}_all.txt", f"/home/decken/Dropbox2/Dropbox/list_{stock_type}_all.txt")
