import os
from util import StockUtil, MysqlUtil

if __name__ == "__main__":
    stock_util = StockUtil()
    mysql_util = MysqlUtil()

    stock_util.update_proxy()

    for filename in os.listdir('GICS'):
        if not filename.endswith(".csv"):
            continue

        print("=== Processing {} ===".format(filename))
        group = filename.split('-')[2].replace('.csv', '')
        good_stock_names = stock_util.get_stock_daily('GICS/' + filename)
        print('stock_util.rs: {}'.format(stock_util.rs))
        print('stock_util.rs_counter: {}'.format(stock_util.rs_counter))
        if stock_util.rs_counter:
            print('stock_util.rs / stock_util.rs_counter: {}'.format(stock_util.rs / stock_util.rs_counter))
        if good_stock_names:
            rs_all = stock_util.rs / stock_util.rs_counter
            gsn = []
            for one in good_stock_names:
                if one['rs'] > rs_all:
                    gsn.append('{}({})'.format(one['stock_name'], round(one['rs'] / rs_all, 2)))
            if gsn:
                stock_util.line_notify(group, gsn)
        # os.rename('GICS/' + filename, 'GICS/finished/' + filename)
        print("--- Finished {} ---".format(filename))

    # print(stock_util.get_one_year_max_min_index(2090))
    # stock_util.retrospect_ma()
    # print(stock_util.transform_m_b_to_number('234k'))
    # print(stock_util.html_get('https://www.marketwatch.com/investing/stock/PTC/download-data?date=12-04-2020'))
    # print(stock_util.update_proxy())
    # (conn, cursor) = mysql_util.create_sql_conn()
    # mysql_util.close_sql_conn(conn, cursor)
    # print(stock_util.get_stock_id('aapl'))
    # print(stock_util.get_stock_info('aapl'))
    # stock_util.line_notify('1', ['2'])
    # stock_util.get_index_volume('.')
    # print(stock_util.get_earning_released_date('aapl'))
    # stock_util.get_Xueqiu_categories()
    # stock_util.extract_expenditure_revenue('aapl')
    # print(stock_util.compute_ma(150, 'AAPL'))