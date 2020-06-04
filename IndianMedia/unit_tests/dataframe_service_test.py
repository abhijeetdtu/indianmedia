import unittest

from IndianMedia.data_process.dataframe_service import DataFrameService

import pandas as pd

class DataFrameServiceTest(unittest.TestCase):

    def setUp(self):
        self.sut = DataFrameService()

    def test_getWordCorrelations(self):
        df = self.sut.getWordCorrelations("modi")
        self.assertTrue(len(df.columns)  > 1)

    def test_getWordDates(self):
        df = self.sut.getWordDates("coronavirus")
        self.assertTrue(len(df.columns) , 1)

    def test_getWordDatesMultiIndex(self):
        df = self.sut.getWordDates("coronavirus")
        idx = pd.IndexSlice
        #print(df.head())
        #print(df.loc[idx["2019-05-01" : "2019-06-01" ,:] , :])

if __name__ == "__main__":
    unittest.main()
