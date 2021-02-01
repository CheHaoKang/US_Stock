from util import StockUtil, MysqlUtil

if __name__ == "__main__":
    stock_util = StockUtil()
    mysql_util = MysqlUtil()

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
    stock_util.extract_expenditure_revenue('aapl')