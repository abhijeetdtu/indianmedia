import unittest
from IndianMedia.load_job.getYoutubeVideoTitle import GetChannelVideoInfo
from IndianMedia.constants import Channels
import datetime

class GetYoutubeVideoTests(unittest.TestCase):

    def test_get_video(self):

        #print(afterDate)
        vids = GetChannelVideoInfo(Channels.THE_PRINT,1 , 1)
        self.assertEqual(len(vids) ,1)


if __name__ == "__main__":
    unittest.main()
