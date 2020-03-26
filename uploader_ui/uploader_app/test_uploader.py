import unittest

from uploader_ui.uploader_app.uploader import FacebookUploaderNoWait, UploadedVideo


class TestFacebookSource(unittest.TestCase):
    def test_accept_ids(self):
        upl = FacebookUploaderNoWait(None, None)
        l = [
            UploadedVideo("1","test1"),
            UploadedVideo("2","test2"),
            UploadedVideo("3","test3"),
            UploadedVideo("4","test4"),
        ]
        upl._index_videos(l)
        self.assertEqual(upl.should_be_uploaded("test1"), False)
        self.assertEqual(upl.should_be_uploaded("test2"), False)
        self.assertEqual(upl.should_be_uploaded("test5"), True)
        self.assertEqual(upl.get_by_name("test2").id, "2")
        upl._delete_from_index(l[1])
        self.assertEqual(upl.get_by_name("test2"), None)
        self.assertEqual(upl.should_be_uploaded("test2"), True)




if __name__ == "__main__":
    unittest.main()