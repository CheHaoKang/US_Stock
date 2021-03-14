import os
from util import StockUtil, MysqlUtil
import shutil

if __name__ == "__main__":
    stock_util = StockUtil()
    mysql_util = MysqlUtil()

    stock_util.test = 0

    if not stock_util.test:
        stock_util.update_proxy()

    if not stock_util.test:
        source_dir = 'GICS'
        finished_dir = 'GICS/finished'
        for file_name in os.listdir(finished_dir):
            if file_name.endswith(".csv"):
                shutil.move(os.path.join(finished_dir, file_name), source_dir)

    folder = '.' if stock_util.test else 'GICS'
    if True:
        stock_util.line_notify()
        for filename in os.listdir(folder):
            if not filename.endswith(('test' if stock_util.test else '') + ".csv"):
                continue

            print("=== Processing {} ===".format(filename))
            group = filename.split('-')[2].replace('.csv', '')
            good_stock_names = stock_util.get_stock_daily('{}/{}'.format(folder, filename))
            print('stock_util.rs: {}'.format(stock_util.rs))
            print('stock_util.rs_counter: {}'.format(stock_util.rs_counter))
            if stock_util.rs_counter:
                print('stock_util.rs / stock_util.rs_counter: {}'.format(stock_util.rs / stock_util.rs_counter))
            if good_stock_names:
                rs_all = stock_util.rs / stock_util.rs_counter
                gsn = []
                for one in good_stock_names:
                    if rs_all <= 0:
                        if one['rs'] > 0.15:
                            gsn.append('{}({})'.format(one['stock_name'], '+'))
                    elif one['rs'] > rs_all * 1.65:
                        gsn.append('{}({})'.format(one['stock_name'], round(one['rs'] / rs_all, 2)))
                if gsn:
                    stock_util.line_notify(group, gsn)
            if not stock_util.test:
                os.rename('GICS/' + filename, 'GICS/finished/' + filename)
            print("--- Finished {} ---".format(filename))

    # stock_util.renew_categories_index_volume()
    # stock_util.get_index_volume(folder)
    # stock_util.retrospect_ma(stock_id=None, days=365)
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