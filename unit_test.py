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