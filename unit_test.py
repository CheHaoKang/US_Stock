import unittest
import util

class StockTestCase(unittest.TestCase):
    def setUp(self):
        print("setUp\n")
        self.args = (3, 2)
        self.addCleanup(self.clean_up, 'clean_up_setUp')

    def tearDown(self):
        print("tearDown\n")
        self.args = None

    def clean_up(self, s):
        print(s)

    def test_plus(self):
        expected = 5
        result = util.StockUtil.plus(*self.args)
        self.assertEqual(expected, result)
        self.addCleanup(self.clean_up, 'clean_up_test_plus')

    def test_minus(self):
        expected = 1
        result = util.StockUtil.minus(*self.args)
        self.assertEqual(expected, result)
        self.addCleanup(self.clean_up, 'clean_up_test_minus')

if __name__ == "__main__":
    suite = unittest.TestSuite()
    suite.addTest(StockTestCase('test_plus'))
    suite.addTest(StockTestCase('test_minus'))
    unittest.main(verbosity=2)

    # def renew_index_volume(self, filename):
        # params = {}
        # if filename:
        #     params['filename'] = filename
        # else:
        #     self.retrieve_all_stocks()
        # self.get_index_volume(**params, skip_stock=True)
        # self.retrospect_ma(stock_id=None, days=self.retrospect_days)

# def get_index_volume(self, path='GICS', filename=None, skip_stock=True):
#         self.cursor.execute('''
#             SELECT  stock_name
#             FROM    stock_list
#             WHERE   id IN (
#                 SELECT  DISTINCT(stock_id)
#                 FROM    index_volume
#             )
#         ''')
#         all_stock_names = [ stock_name['stock_name'] for stock_name in self.cursor.fetchall() ]
#         self.new_stocks = []

#         url = 'https://www.marketwatch.com/investing/stock/{0}/download-data'

#         import csv
#         import pandas as pd
#         from io import StringIO

#         for file_name in os.listdir(path):
#             if file_name.endswith(('test' if self.test else '') + ".csv"):
#                 if filename and filename != file_name:
#                     continue
#                 print("===Processing {}===".format(file_name))

#                 with open(path + '/' + file_name, encoding="utf8", newline='') as csvfile:
#                     rows = csv.reader(csvfile, delimiter=',')
#                     headers = next(rows, None)
#                     for row in rows:
#                         for idx, col in enumerate(row, start=0):
#                             if not col or '/' not in col:
#                                 continue

#                             stock_name = col.split('/')[-1]
#                             if not stock_name:
#                                 continue
#                             elif skip_stock and stock_name in all_stock_names:
#                                 print("Skipped {}...".format(stock_name))
#                                 continue
#                             print("\nProcessing {}".format(stock_name))

#                             download_link = None
#                             counter = 0
#                             while counter < self.RETRY:
#                                 counter += 1

#                                 html = self.html_get(url.format(stock_name), stock_name=stock_name, all_stock_names=all_stock_names, verify_item='MW_Masthead_Logo')
#                                 if html != -1:
#                                     soup = BeautifulSoup(html, 'html.parser')
#                                     for a_href in soup.findAll("a"):
#                                         if a_href.text == 'Download Data (.csv)':
#                                             download_link = a_href['href']
#                                             break
#                                 if download_link:
#                                     break
#                             if counter >= self.RETRY:
#                                 print("*** Failed: {} ***\n".format(stock_name))
#                                 self.add_failed_stock(stock_name)
#                                 time.sleep(0.3)
#                                 continue

#                             params = parse_qs(urlparse(download_link).query)
#                             params['startdate'][0] = (datetime.now() - timedelta(days=self.retrospect_days)).strftime("%m/%d/%Y 00:00:00")
#                             params['enddate'][0] = datetime.now().strftime("%m/%d/%Y 23:59:59")
#                             download_link = "{}?{}".format(download_link[:download_link.find('?')], urlencode(params, doseq=True))
#                             print("download_link: {}".format(download_link))

#                             stock_id = self.get_stock_id(stock_name)
#                             print("stock_id: {}".format(stock_id))
#                             self.new_stocks.append(stock_id)

#                             try:
#                                 content = self.html_get(download_link, stock_name=stock_name, all_stock_names=all_stock_names)
#                                 df = pd.read_csv(StringIO(content))
#                                 rows = []
#                                 for _, row in df.iterrows():
#                                     dt = self.mdy_to_ymd(row['Date'])
#                                     idx = float(str(row['Close']).replace('$', '').replace(',', ''))
#                                     idx_open = float(str(row['Open']).replace('$', '').replace(',', ''))
#                                     idx_low = float(str(row['Low']).replace('$', '').replace(',', ''))
#                                     idx_high = float(str(row['High']).replace('$', '').replace(',', ''))
#                                     volume = self.transform_m_b_to_number(str(row['Volume']).replace(',', ''))
#                                     rows.append((dt, stock_id, volume, idx, idx_open, idx_low, idx_high))
#                                 self.cursor.executemany(self.insert_index_volume_sql, rows)
#                                 self.conn.commit()
#                             except:
#                                 sys.stderr.write("*** {} ***\n".format(stock_name))
#                                 traceback.print_exc()
#                                 self.add_failed_stock(stock_name)

#                                 # print("*** Deleting {} ({}) ***".format(stock_name, stock_id))
#                                 # delete_stock_id_sql = 'DELETE FROM stock_list WHERE id = %s'
#                                 # self.cursor.execute(delete_stock_id_sql, (stock_id))
#                                 # delete_index_sql = 'DELETE FROM index_volume WHERE stock_id = %s'
#                                 # self.cursor.execute(delete_index_sql, (stock_id))
#                                 # self.conn.commit()

#                                 sys.stderr.write("--- {} ---\n".format(stock_name))

#                             self.retrospect_ma(stock_id=stock_id, days=self.retrospect_days)

#                 print("---Finished {}---".format(file_name))



                    # print("\n==={} {}".format(stock_name, day))
                    # print(stocks_data[stock_name][day])
                    # print("avg_volume: {}".format(avg_volume))
                    # print("day volume: {}".format(stocks_data[stock_name][day]['volume']))
                    # print("less than avg_volume*{}: {}".format(self.volume_ratio, stocks_data[stock_name][day]['volume'] <= float(avg_volume) * self.volume_ratio))
                    # print("qualified_year_max_min: {}".format(self.qualified_year_max_min(stock_id, stocks_data[stock_name][day]['index'])))
                    # print("index bigger than ma_50*{}: {}".format(self.ratio_ma50, stocks_data[stock_name][day]['index'] >= stocks_data[stock_name][day]['ma_50'] * self.ratio_ma50))
                    # print("ma_50 bigger than ma_150: {}".format(stocks_data[stock_name][day]['ma_50'] >= stocks_data[stock_name][day]['ma_150']))
                    # print("ma_50 bigger than ma_200: {}".format(stocks_data[stock_name][day]['ma_50'] >= stocks_data[stock_name][day]['ma_200']))
                    # print("---{} {}\n".format(stock_name, day))

                    # if stocks_data[stock_name][day]['volume'] <= float(avg_volume) * self.volume_ratio and self.qualified_year_max_min(stock_id, stocks_data[stock_name][day]['index']) and stocks_data[stock_name][day]['index'] >= stocks_data[stock_name][day]['ma_50'] * self.ratio_ma50 and (stocks_data[stock_name][day]['ma_50'] >= stocks_data[stock_name][day]['ma_150'] * self.ratio_ma150 or stocks_data[stock_name][day]['ma_50'] >= stocks_data[stock_name][day]['ma_200'] * self.ratio_ma200):
                    #     qualified_days += 1

                    ### day_minus_one = (datetime.strptime(day, '%Y-%m-%d') - timedelta(1)).strftime("%Y-%m-%d")
                    ### day_minus_one_ma_60 = stocks_data[stock_name][day_minus_one]['ma_60'] if day_minus_one in stocks_data[stock_name] and 'ma_60' in stocks_data[stock_name][day_minus_one] else 0
                    # if stocks_data[stock_name][day]['volume'] >= float(avg_volume) * self.volume_ratio and self.qualified_year_max_min(stock_id, stocks_data[stock_name][day]['index']) and stocks_data[stock_name][day]['index'] >= stocks_data[stock_name][day]['ma_5'] and stocks_data[stock_name][day]['ma_5'] >= stocks_data[stock_name][day]['ma_10'] and stocks_data[stock_name][day]['ma_20'] >= stocks_data[stock_name][day]['ma_60'] and stocks_data[stock_name][day]['ma_60'] > day_minus_one_ma_60:
                    ###     qualified_days += 1