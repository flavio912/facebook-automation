import unittest

from uploader_ui.uploader_app.pattern import is_file_match


class TestPattern(unittest.TestCase):
    def test_pattern(self):
        examples = (
            (True, "Channel=1_Platform=1_Creative-Theme=4_Template=T14_Job=320_Version-Opener=2_Length=27_Copy=3_Creator=4_Gender-Targeting=All_Geo-Targeting=US_Language-Targeting=Eng_Age-Targeting=1_Interest-Targeting=2_Actor-Gender=F_Actor-Age=26_Actor-Demo=3.mp4"),
            (True, "Channel=1_Platform=2_Creative-Theme=4_Template=T14_Job=320_Version-Opener=2_Length=27_Copy=3_Creator=4_Gender-Targeting=All_Geo-Targeting=US_Language-Targeting=Eng_Age-Targeting=1_Interest-Targeting=2_Actor-Gender=F_Actor-Age=26_Actor-Demo=3.mp4"),
            (True, "Channel=1_Platform=1_Creativete=T14_Job=320Version-Opener=2_Length=27_Copy=3_Creator=4_Gender-Targeting=All_Geo-Targeting=US_Language-Targeting=Eng_Age-Targeting=1_Interest-Targeting=2_Actor-Gender=F_Actor-Age=26_Actor-Demo=3.mp4"),
            (True, "Channel=1_Platform=1_Creative-Theme=4_Template=T14_Job=320_Version-Opener=2_Length=27_Copy=3_Creator=4_Gender-Targeting=All_Geo-Targeting=US_Language-Targeting=Eng_Age-Targeting=1_Interest-Targeting=2_Actor-Gender=F_Actor-Age=26_Actor-Demo=3.mp4"),
            (False, "Folder"),
            (False, ""),
            (False, "."),
            (False, ".."),
            (False, "=1_Platform=1_Creative-Theme=4_Template=T14_Job=320_Version-Opener=2_Length=27_Copy=3_Creator=4_Gender-Targeting=All_Geo-Targeting=US_Language-Targeting=Eng_Age-Targeting=1_Interest-Targeting=2_Actor-Gender=F_Actor-Age=26_Actor-Demo=3.mp4"),
            (False, "0_Version-Opener=2_Length=27_Copy=3_Creator=4_Gender-Targeting=All_Geo-Targeting=US_Language-Targeting=Eng_Age-Targeting=1_Interest-Targeting=2_Actor-Gender=F_Actor-Age=26_Actor-Demo=3.mp4"),
            (False, "US_Language-Targeting=Eng_Age-Targeting=1_Interest-Targeting=2_Actor-Gender=F_Actor-Age=26_Actor-Demo=3.mp4"),
        )
        for (res, str) in examples:
            self.assertEqual(is_file_match(str), res, str)


if __name__ == "__main__":
    unittest.main()