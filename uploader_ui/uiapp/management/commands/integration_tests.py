import json
import os
from django.core.management import BaseCommand
from facebook_business import FacebookSession, FacebookAdsApi

from uploader_app.app import Uploader
from uploader_app.source import DropBoxSource
from uploader_app.uploader import FacebookUploaderNoWait
from ...appstorage import DjangoStorage

class Command(BaseCommand):
    help = 'Integr Test'

    def _print(self, s):
        self.stdout.write(f"{s}\n")

    def handle(self, *args, **options):
        session = FacebookSession(
            os.environ['FB_GA_APPID'],
            os.environ['FB_GA_APPKEY'],
            os.environ['FB_GA_TOKEN'],
        )
        act_id = os.environ['FB_ACT_ID']
        tmp_dir = os.path.abspath(os.environ['GA_TEMP_DIR'])

        uploader = FacebookUploaderNoWait(FacebookAdsApi(session), act_id)
        storage = DjangoStorage()
        source = DropBoxSource(os.environ['DROPBOX_TOKEN'],
                               os.environ['GA_ROOT'])

        try:
            uploader.index()
            i = 5
            for f in source.get_files():
                i-=1
                if i == 0:
                    break

            self._print("All tests completed successfully")
        except Exception as e:
            self._print(f"Error: {str(e)}")
