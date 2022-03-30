import os
from util import StockUtil, MysqlUtil
import shutil
import sys
import time

def move_directory(source, destination):
    for file_name in os.listdir(source):
        if file_name.endswith(".csv"):
            shutil.move(os.path.join(source, file_name), destination)

if __name__ == "__main__":
    stock_util = StockUtil()
    mysql_util = MysqlUtil(stock_util.conn, stock_util.cursor)

    stock_util.test = 0
    # stock_util.stagnate = 1 if len(sys.argv) >= 2 and sys.argv[1] == '--stagnate' else 0
    stock_util.stagnate = 0
    stock_util.get_new_stocks = 0

    if stock_util.get_new_stocks:
        if os.path.exists('GICS/backup'):
            shutil.rmtree('GICS/backup')
        os.makedirs('GICS/backup')
        move_directory('GICS', 'GICS/backup')
        stock_util.renew_categories_index_volume()
        shutil.rmtree('GICS/finished')

        sys.exit(0)

    if not (stock_util.test or stock_util.stagnate):
        stock_util.update_proxy()

    # if interrupted, comment out this block
    if not stock_util.test:
        move_directory('GICS/finished', 'GICS')

    folder = '.' if stock_util.test else 'GICS'
    if True:
        if not stock_util.get_stock_days():
            print('Stock market closed yesterday.')
            sys.exit(0)

        stock_util.line_notify()
        for filename in os.listdir(folder):
            if not filename.endswith(('test' if stock_util.test else '') + ".csv"):
                continue

            print("=== Processing {} ===".format(filename))
            group = filename.split('-')[2].replace('.csv', '')
            if stock_util.stagnate:
                stagnating_stocks = stock_util.find_stagnate_stocks('{}/{}'.format(folder, filename))
                if stagnating_stocks:
                    print('>>> Stagnating')
                    print("\n".join(stagnating_stocks))
            else:
                stagnating_stocks = stock_util.find_stagnate_stocks('{}/{}'.format(folder, filename))
                good_stock_names = stock_util.get_stock_daily('{}/{}'.format(folder, filename))
                # continue
                print('rs: {}'.format(stock_util.rs))
                print('rs_counter: {}'.format(stock_util.rs_counter))
                if stock_util.rs_counter:
                    rs_all = stock_util.rs / stock_util.rs_counter
                    print('rs / rs_counter: {}'.format(rs_all))
                    print('rs / rs_counter * {}: {}'.format(1 + stock_util.rs_ratio, rs_all * (1 + stock_util.rs_ratio)))
                if good_stock_names:
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
                    if gsn:
                        stock_util.line_notify(group, gsn)
                if not stock_util.test:
                    os.rename('GICS/' + filename, 'GICS/finished/' + filename)
            print("--- Finished {} ---".format(filename))

    # stock_util.renew_categories_index_volume()
    # stock_util.retrieve_zero_ma_date()
    # print(stock_util.get_one_year_max_min_index(2090))
    # print(stock_util.transform_m_b_to_number('234k'))
    # print(stock_util.html_get('https://www.marketwatch.com/investing/stock/PTC/download-data?date=12-04-2020'))
    # print(stock_util.update_proxy())
    # (conn, cursor) = mysql_util.create_sql_conn()
    # mysql_util.close_sql_conn(conn, cursor)
    # print(stock_util.get_stock_id('aapl'))
    # print(stock_util.get_stock_info('aapl'))
    # stock_util.line_notify('1', ['2'])
    # print(stock_util.get_earning_released_date('aapl'))
    # stock_util.get_Xueqiu_categories()
    # stock_util.extract_expenditure_revenue('aapl')
    # print(stock_util.compute_ma(150, 'AAPL'))
