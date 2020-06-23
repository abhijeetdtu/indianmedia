import unittest

from IndianMedia.load_job.mongoToCSV import getDF

class MongoToCSVTest(unittest.TestCase):

    def test_getDF(self):
        df = getDF()
        self.assertEqual(len(df.columns) , 6)

if __name__ == "__main__":
    unittest.main()
