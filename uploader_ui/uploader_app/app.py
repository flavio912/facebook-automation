import logging
import os
import time

# from facebook_business import FacebookAdsApi
from facebook_business.adobjects.abstractcrudobject import AbstractCrudObject
from facebook_business.exceptions import FacebookBadObjectError, FacebookRequestError
from facebook_business.adobjects.adset import AdSet
from facebook_business.adobjects.targeting import Targeting
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.ad import Ad
from facebook_business.adobjects.adcreative import AdCreative
from facebook_business.adobjects.advideo import AdVideo
from facebook_business.adobjects.campaign import Campaign
from facebook_business import FacebookSession, FacebookAdsApi

from .pattern import is_file_match
from .source import SourceBase, FileInfoBase
from .storage import StorageBase
from .uploader import UploaderBase, TooManyCallsError
import re
import json


from multiprocessing.dummy import Pool as ThreadPool

def get_parent_id(self):
    """
    There is no way no avoid parent_id using, since there is not such options for VideoUploader
    And also facebook SDK uses root logger so it's impossible to disable their warnings
    thats' why i'm redifined this method
    :param self:
    :return:
    """
    #warning_message = "parent_id is being deprecated."
    #logging.warning(warning_message)
    """Returns the object's parent's id."""
    return self._parent_id or FacebookAdsApi.get_default_account_id()

def get_parent_id_assured(self):
    """Returns the object's parent's fbid.
    Raises:
        FacebookBadObjectError if the object does not have a parent id.
    """
    #warning_message = "parent_id is being deprecated."
    #logging.warning(warning_message)
    if not self.get_parent_id():
        raise FacebookBadObjectError(
            "%s object needs a parent_id for this operation."
            % self.__class__.__name__,
        )

    return self.get_parent_id()

AbstractCrudObject.get_parent_id = get_parent_id
AbstractCrudObject.get_parent_id_assured = get_parent_id_assured

# start_folder = '/Jobs_Dev'
# start_folder = '/Jobs_Dev/J343_Survivor_RoofMoneyCount/Exports/T7/V2'

class Uploader:
    def __init__(self,
                 storage: StorageBase,
                 source: SourceBase,
                 uploader: UploaderBase,
                 tmp_dir: str
                 ):
        self._storage = storage
        self._source = source
        self._uploader = uploader
        self._tmp_dir = tmp_dir

    def _handle_file(self, session_id: id, file: FileInfoBase):
        logging.debug(f"_handle_file session_id:{session_id} file.name={file.name} file.path={file.path}")
        is_matched = is_file_match(file.name)
        if is_matched:#and self._uploader.should_be_uploaded(file.name):
            new_file = os.path.join(self._tmp_dir, file.name)
            logging.info(f"Downloading: {file.name}")

            # Todo. if the new_file is already exist, it's not need to download again.
            # <<
            # input: new_file
            # output: check it's existence.
            # >>
            self._source.download_file(file, new_file)
            logging.info(f"Successful download, uploading: {file.name}")

            uploaded = self._uploader.upload_to_campaign(file.name, file.path, file.job_number, new_file)
            if uploaded is not None:
                #self._storage.create_video(session_id, uploaded.id, file.name, file.path)
                logging.info(f"Successful upload to Campaign: {file.name}")
                return uploaded, None
            else:
                #self._storage.create_video(session_id, uploaded.id, file.name, file.path, "error")
                logging.warning(f"Upload to Campaign failed: {file.name}")
                return None, file

            # uploaded = None;
            # uploaded = self._uploader.upload(new_file)
            # if uploaded is not None:
            #     self._storage.create_video(session_id, uploaded.id, file.name, file.path)
            #     logging.info(f"Successful upload: {file.name}")
            #     return uploaded, None
            # else:
            #     self._storage.create_video(session_id, uploaded.id, file.name, file.path, "error")
            #     logging.warning(f"Upload failed: {file.name}")
            #     return None, file
        else:
            logging.info(f"Skip: {file.name}")
            return None, None

    def _do_index(self):
        self._uploader.index()
        return True

    def run(self):
        # Test Code <<
        FacebookAdsApi.init(os.environ['FB_GA_APPID'], os.environ['FB_GA_APPKEY'], os.environ['FB_GA_TOKEN'])
        account = AdAccount('act_659750741197329')

        campaigns = account.get_campaigns(fields=[
            Campaign.Field.name,
            Campaign.Field.id
        ])

        campaign = None
        for r in campaigns:
            if r['name'] == 'US-AND-MAI-ABO-J606_MagicSink': #'US-AND-MAI-ABO-J': #
                campaign = r

        print(campaign)

        adgroups = campaign.get_ad_sets(fields=[
            AdSet.Field.name,
            AdSet.Field.id
        ])

        param_types = {
            'deep_copy': True,
            'rename_options': 'Object',
            'status_option': 'ACTIVE',
        }
        # campaign.create_copy()
        for r in adgroups:
            if r['name'] == 'Creative-Theme=2_Template=T7-4_Job=606_Version-Opener=1_Copy=1_Creator=7_Gender=none_Age=0_Demo=9':    #'Test':
                ad_set = r

        print(ad_set)
        ads = ad_set.get_ads(fields=[
            Ad.Field.name,
            Ad.Field.configured_status,
            Ad.Field.creative,
        ])
        print(ads)

        ad = ads[0]
        params = {
            "adset_id": ad_set['id'],
            "status_option": 'ACTIVE',
            "rename_options": {
                "rename_strategy" : "DEEP_RENAME",
                #"rename_prefix" : "",
                #"rename_suffix" : ""
            }
        }
        ad.create_copy(params=params)

        session = FacebookSession(
            os.environ['FB_GA_APPID'],
            os.environ['FB_GA_APPKEY'],
            os.environ['FB_GA_TOKEN'],
        )
        _api = FacebookAdsApi(session)

        video = AdVideo(api=_api)
        video._parent_id = "act_659750741197329"
        video[AdVideo.Field.filepath] = "C:\\GA_TEMP_DIR\\test.mp4"

        #res = video.remote_create()
        video_id_for_creative = video.get_id()
        print(video)
        print(video_id_for_creative)

        fields = [
            AdCreative.Field.account_id,
            AdCreative.Field.actor_id,
            AdCreative.Field.adlabels,
            AdCreative.Field.applink_treatment,
            AdCreative.Field.asset_feed_spec,
            AdCreative.Field.authorization_category,
            AdCreative.Field.auto_update,
            AdCreative.Field.body,
            AdCreative.Field.branded_content_sponsor_page_id,
            AdCreative.Field.bundle_folder_id,
            AdCreative.Field.call_to_action_type,
            AdCreative.Field.categorization_criteria,
            AdCreative.Field.category_media_source,
            AdCreative.Field.destination_set_id,
            AdCreative.Field.dynamic_ad_voice,
            AdCreative.Field.effective_authorization_category,
            AdCreative.Field.effective_instagram_media_id,
            AdCreative.Field.effective_instagram_story_id,
            AdCreative.Field.effective_object_story_id,
            # AdCreative.Field.enable_direct_install,
            # AdCreative.Field.enable_launch_instant_app,
            AdCreative.Field.id,
            AdCreative.Field.image_crops,
            AdCreative.Field.image_hash,
            AdCreative.Field.image_url,
            AdCreative.Field.instagram_actor_id,
            AdCreative.Field.instagram_permalink_url,
            AdCreative.Field.instagram_story_id,
            AdCreative.Field.interactive_components_spec,
            AdCreative.Field.link_deep_link_url,
            AdCreative.Field.link_destination_display_url,
            AdCreative.Field.link_og_id,
            AdCreative.Field.link_url,
            AdCreative.Field.messenger_sponsored_message,
            AdCreative.Field.name,
            AdCreative.Field.object_id,
            AdCreative.Field.object_store_url,
            AdCreative.Field.object_story_id,
            AdCreative.Field.object_story_spec,
            AdCreative.Field.object_type,
            AdCreative.Field.object_url,
            AdCreative.Field.place_page_set_id,
            AdCreative.Field.platform_customizations,
            AdCreative.Field.playable_asset_id,
            AdCreative.Field.portrait_customizations,
            AdCreative.Field.product_set_id,
            AdCreative.Field.recommender_settings,
            AdCreative.Field.status,
            AdCreative.Field.template_url,
            AdCreative.Field.template_url_spec,
            AdCreative.Field.thumbnail_url,
            AdCreative.Field.title,
            AdCreative.Field.url_tags,

            # AdCreative.Field.use_page_actor_override,
            # AdCreative.Field.video_id,
            # AdCreative.Field.call_to_action,
            # AdCreative.Field.image_file,
            # AdCreative.Field.is_dco_internal
        ]

        adcreatives = account.get_ad_creatives(fields=fields)
        for c in adcreatives:
            if c['id'] == '23844416112710002':
                adcreative = c
        print(adcreative)

        #adcreative = AdCreative('23844416112710002')
        #exist_params = adcreative.remote_read()
        #exist_params = adcreative.remote_read(fields=fields)


        params = {
            "account_id": "659750741197329",
            "actor_id": "628994917168628",
            "instagram_actor_id": "677652652254752",
            "instagram_permalink_url": "https://www.instagram.com/p/B-Pug4wANTH/",

            "name": "ENTER CREATIVE NAME HERE",
            "video_id": video_id_for_creative,
            "object_type": 'SPONSORED_VIDEO',
            "body": "\"I play every morning. <3 <3 <3 this app!\" - Parker C.",
            "effective_authorization_category": "NONE",
            "effective_instagram_media_id": "17877400444568435",
            "effective_instagram_story_id": "2676891812440276",
            "effective_object_story_id": "628994917168628_2778221908912574",
            "object_story_spec": {
                "instagram_actor_id": "677652652254752",
                "page_id": "628994917168628",
                "link_data": {
                    'link': 'http://play.google.com/store/apps/details?id=com.luckyday.app',
                    'message': 'ENTER AD MESSAGE HERE',
                    "call_to_action": {
                        "type": "PLAY_GAME",
                        "value": {
                            "link": "http://play.google.com/store/apps/details?id=com.luckyday.app"
                        }
                    }
                }
            }
        }

        // Duplicate
        adcreative = account.create_ad_creative(params=params)
        print(adcreative)

        params = {
            Ad.Field.name: 'ENTER AD NAME HERE',
            Ad.Field.campaign_id: campaign['id'],
            Ad.Field.adset_id: ad_set['id'],
            Ad.Field.creative: {'creative_id': adcreative['id']}, #'23844416112710002'}, #23844416105030002
            Ad.Field.status: 'ACTIVE'}

        finish = account.create_ad(params=params)
        print(finish)
        print("Finished")
        return
        # >>

        logging.info("Indexing uploader...")
        if not self._do_index():
            logging.warning("index unsuccessful")
            return
        logging.info("Indexing done. Started scanning source")
        session_id = self._storage.create_session_id()

        parallelism = int(os.getenv('PARALLELISM', '5'))
        logging.info(f'Using {parallelism} threads to upload files')
        pool = ThreadPool(parallelism)
        try:

            total_uploaded = []
            logging.info(f'Enumerating files in {self._source._start_folder}')
            all_files = list(self._source.get_files())
            logging.info(f'Found {len(all_files)} total files in {self._source._start_folder}')
            files = [f for f in all_files if is_file_match(f.filename)]
            logging.info(f'Found {len(files)} matching files in {self._source._start_folder}')

            while True:
                results = pool.map(lambda file: self._handle_file(session_id, file), files)
                pool.close()
                pool.join()
                not_uploaded_files = list(filter(lambda x: x is not None, map(lambda x: x[1], results)))
                uploaded_videos = list(filter(lambda x: x is not None, map(lambda x: x[0], results)))
                total_uploaded += uploaded_videos
                if len(not_uploaded_files) > 0:
                    files = not_uploaded_files
                    logging.info(f"Not uploaded files: {len(not_uploaded_files)}. Retry")
                else:
                    break

            self._uploader.set_uploaded_videos(total_uploaded)
            logging.info(f"{total_uploaded} files uploaded. Waiting for processing completion")
            for id, status in self._uploader.wait_all():
                self._storage.update_video_status(id, status)
            logging.info(f"Done")
            self._storage.session_completed(session_id)
        except Exception as e:
            logging.exception(str(e))
            self._storage.session_completed_error(session_id, str(e))
