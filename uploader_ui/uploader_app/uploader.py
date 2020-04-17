import logging
import os
import time
from http import HTTPStatus
from typing import List, Optional, Generator, Tuple

from facebook_business import FacebookSession, FacebookAdsApi
from facebook_business.adobjects.advideo import AdVideo
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.api import FacebookResponse, Cursor
from facebook_business.exceptions import FacebookRequestError
from facebook_business.adobjects.campaign import Campaign
from facebook_business.adobjects.adset import AdSet
from facebook_business.adobjects.ad import Ad
from facebook_business.adobjects.adcreative import AdCreative

VIDEO_STATUS_READY = 'ready'

class TooManyCallsError(Exception):
    pass


class UploadedVideo:
    def __init__(self, id, name=None, status=None):
        self.id = id
        self.name = name
        self.status = status


class UploaderBase:
    def index(self):
        raise NotImplementedError

    def should_be_uploaded(self, video_name: str) -> bool:
        raise NotImplementedError

    def get_by_id(self, video_id: str) -> Optional[UploadedVideo]:
        raise NotImplementedError

    def get_by_name(self, video_name: str) -> Optional[UploadedVideo]:
        raise NotImplementedError

    def index_campaigns(self):
        raise NotImplementedError

    def get_campaign_by_name(self, campaigns, name: str):
        raise NotImplementedError

    def get_adset_by_name(self, ads, name: str):
        raise NotImplementedError

    def get_ad_by_name(self, ads, name: str):
        raise NotImplementedError

    def get_video_by_name(self, videos, name: str):
        raise NotImplementedError

    def get_adset_name_from_path(self, name:str, path:str):
        raise NotImplementedError

    def get_campaign_name_from_path(self, name:str, path:str):
        raise NotImplementedError

    def upload(self, path: str) -> Optional[UploadedVideo]:
        raise NotImplementedError

    def is_video_uploaded(self, name: str):
        raise NotImplementedError

    def duplicate_campaign(self, template_id, job_num, job_name):
        raise NotImplementedError

    def duplicate_adset(self, campaign, adset_name):
        raise NotImplementedError

    def duplicate_ad(self, ad_set, uploaded_video, video_id, video_name):
        raise NotImplementedError

    def create_ad_with_duplicate(self, path: str, name: str, job_num: int, template_id : str):
        raise NotImplementedError

    def set_uploaded_videos(self, files: List[UploadedVideo]):
        raise NotImplementedError

    def wait_all(self) -> Generator[Tuple[str,str], None, None]:
        """
        Yields file ids with new statuses
        :return:
        """
        raise NotImplementedError

    def delete_video(self, video: UploadedVideo) -> bool:
        raise NotImplementedError

    def reload(self, video: UploadedVideo):
        raise NotImplementedError


class FacebookUploaderNoWait(UploaderBase):
    """
    Uploads file to facebook servers but does not waits for video.waitUntilEncodingReady()
    """
    def __init__(self, api: FacebookAdsApi, act_id: str):
        self._api = api
        self._act_id = act_id
        self._act = AdAccount(act_id, None, api)
        self._index = {}  # hash map of existing videos
        self._index_ids = {}  # hash map of existing videos
        self._uploaded_videos = {}
        self._campaigns = []

    def _index_videos(self, videos: List[UploadedVideo]):
        self._index.update(dict(zip(list(map(lambda x: x.name, videos)), videos)))
        self._index_ids.update(dict(zip(list(map(lambda x: x.id, videos)), videos)))

    def _delete_from_index(self, video: UploadedVideo):
        if video.id in self._index_ids:
            del self._index_ids[video.id]
        if video.name in self._index:
            del self._index[video.name]

    def _resp_to_video(self, resp: dict) -> UploadedVideo:
        return UploadedVideo(resp['id'],
                             resp['title'] if 'title' in resp else None,
                             resp['status']['video_status'] if 'status' in resp and 'video_status' in resp['status'] else None
                             )
    def _resp_to_video2(self, v: AdVideo) -> UploadedVideo:
        return self._resp_to_video(v._data)

    def _get_exception_description(self, e: FacebookRequestError):
        if e.body() is not None and 'error' in e.body() and 'message' in e.body()['error']:
            return e.body()['error']['message']
        return "no descriptions"

    def _is_too_many_calls_exception(self, e: FacebookRequestError):
        return "#80004" in self._get_exception_description(e)

    def _decode_request_error(self, e: FacebookRequestError):
        err = f"facebook request failed: {self._get_exception_description(e)}"
        if self._is_too_many_calls_exception(e):
            raise TooManyCallsError(err)
        raise Exception(err)

    def index(self):
        limit = int(os.getenv('FB_AD_PAGE_SIZE','100'))
        logging.info(f'starting indexing for account={self._act_id} with limit={limit}')
        try:
            c = Cursor(self._act, AdVideo, fields=[AdVideo.Field.title,AdVideo.Field.status], params={"limit":limit})
            self._index_videos(list(map(self._resp_to_video2, c)))
            logging.info(f'Indexed {len(self._index_ids)} videos for account {self._act_id}')
        except FacebookRequestError as e:
            logging.warn(f'Failed indexing account {self._act_id}. Error={e}')
            self._decode_request_error(e)

    def should_be_uploaded(self, video_name: str) -> bool:
        return self.get_by_name(video_name) is None

    def get_by_id(self, video_id: str) -> Optional[UploadedVideo]:
        if video_id in self._index_ids:
            return self._index_ids[video_id]
        return None

    def get_by_name(self, video_name: str) -> Optional[UploadedVideo]:
        if video_name in self._index:
            return self._index[video_name]
        return None

    def index_campaigns(self):
        campaigns = self._act.get_campaigns(fields=[
            Campaign.Field.name,
            Campaign.Field.id
        ])
        # _campaigns.update(dict(zip(list(map(lambda c: c["name"], campaigns)), campaigns)))
        for campaign in campaigns:
            self._campaigns.append(campaign)

    def get_campaign_by_name(self, campaigns, name: str):
        for campaign in self._campaigns:
            if campaign["name"] == name:
                return campaign
        return None

    def get_adset_by_name(self, ad_sets, adset_name: str):
        for ad_set in ad_sets:
            if ad_set["name"] == adset_name:
                return ad_set
        return None

    def get_ad_by_name(self, ads, name: str):
        for ad in ads:
            if ad["name"] == name:
                return ad

        return None

    def get_adset_name_from_path(self, name:str, path:str):
        file_path = path.replace('/' + name, "")
        file_path = file_path[file_path.rfind('/') + 1:]
        file_path = file_path[file_path.find('_') + 1:]
        return file_path

    def get_campaign_name_from_path(self, name:str, path:str):
        return path[path.rfind('/') + 1:]

    def delete_video(self, video: UploadedVideo) -> bool:
        r = self._api.call("DELETE", (video.id,))
        if r.is_success():
            return True
        elif r.status() == HTTPStatus.NOT_FOUND:
            return False
        logging.warning(r.json())
        return False

    def reload(self, video: UploadedVideo):
            r = self._api.call("GET", (video.id, ), {
                "fields": "status,title"
            })
            if r.status() == HTTPStatus.NOT_FOUND:
                if video.name in self._index:
                    self._delete_from_index(video)
                return
            if r.is_success():
                self._index_videos([self._resp_to_video(r.json())])
                return
            logging.warning(r.json())

    def is_video_uploaded(self, name: str):
        video = self.get_by_name(name)
        if video is None:
            for id in self._uploaded_videos:
                uploaded_video = self._uploaded_videos[id]
                if name == uploaded_video[AdVideo.Field.title]:
                    return uploaded_video[AdVideo.Field.id]
        else:
            return video[AdVideo.Field.id]
        logging.info(f'Video is not uploaded yet:"{name}"')

        return None

    def duplicate_campaign(self, template_id, job_num, job_name):
        template = Campaign(template_id)
        template = template.api_get(fields=[Campaign.Field.name])
        campaign_name = template[Campaign.Field.name] + str(job_num) + "_" + job_name
        logging.debug(f'Checking Campaign existence: "{campaign_name}"')
        campaigns = self._act.get_campaigns(fields=[
            Campaign.Field.name,
            Campaign.Field.id
        ])
        campaign = self.get_campaign_by_name(campaigns, campaign_name)
        if campaign is not None:
            logging.info(f'Skip Campaign Duplicate: "{campaign_name}"')
        else:
            logging.info(f'Campaign Duplicating: "{campaign_name}"')
            cr = template.create_copy(fields=None, params={'deep_copy': True})
            campaign = Campaign(cr._data['copied_campaign_id'])
            campaign = campaign.api_get(fields=[Campaign.Field.id, Campaign.Field.name])
            campaign.api_update(params={Campaign.Field.name: campaign_name})

            # check campaign is valid
            if campaign is not None and isinstance(campaign, Campaign):
                logging.info(f'Campaign Duplicate success: "{campaign_name}"')
            else:
                return None

        return campaign

    def duplicate_adset(self, campaign, adset_name):
        ad_sets = campaign.get_ad_sets(fields=[
            AdSet.Field.name,
            AdSet.Field.id
        ])
        logging.debug(f'Checking AdSet existence: "{adset_name}"')
        ad_set = self.get_adset_by_name(ad_sets, adset_name)
        if ad_set is not None and isinstance(ad_set, AdSet):
            logging.info(f'Skip AdSet Duplicate: "{adset_name}"')
        else:
            logging.info(f'AdSet Duplicating: "{adset_name}"')
            ad_sets = campaign.get_ad_sets(fields=[ # get again
                AdSet.Field.name,
                AdSet.Field.id
            ])
            adsett = ad_sets[0]
            asr = adsett.create_copy(params={'deep_copy': True})
            ad_set = AdSet(asr._data['copied_adset_id'])
            ad_set.api_update(params={AdSet.Field.name: adset_name})  # rename adset

            if ad_set is not None and isinstance(ad_set, AdSet):
                logging.debug(f'AdSet Duplicate success: "{adset_name}"')
            else:
                logging.debug('Unable to create AdSet: "{adset_name}"')
                return None;

        return ad_set;

    def duplicate_ad(self, ad_set, uploaded_video, video_id, video_name):
        ads = ad_set.get_ads(fields=[Ad.Field.id, Ad.Field.name, Ad.Field.creative])

        logging.debug(f'Checking Ad existence: "{video_name}"')
        ad = self.get_ad_by_name(ads, video_name)
        if ad is not None and isinstance(ad, Ad):
            logging.info(f'Skip Ad: "{video_name}"')
            return True
        else:
            logging.info(f'Ad Updating: "{video_name}"')
            ads = ad_set.get_ads(fields=[Ad.Field.id, Ad.Field.name, Ad.Field.creative])    # get again
            if not len(ads) == 1:
                logging.debug(f'Template Ad should only be one.')
                return None;
            logging.info(f'Creating AdCreative..')
            ad = ads[0]
            ad[Ad.Field.name] = video_name
            ad.api_update(params={Ad.Field.name: video_name})

            adc = AdCreative(ad[Ad.Field.creative][AdCreative.Field.id])
            adc = adc.api_get(fields=[AdCreative.Field.object_story_spec])

            spec = adc[AdCreative.Field.object_story_spec]
            spec['video_data']['video_id'] = video_id
            spec['video_data']['image_url'] = uploaded_video.get_thumbnails(fields=[], params={})[0]['uri']
            spec['video_data'].pop('image_hash')

            new_adc = self._act.create_ad_creative(params={
                'name': video_name,
                'object_story_spec': spec
            })
            if new_adc is not None and isinstance(new_adc, AdCreative):
                logging.info(f'AdCreative create success(id): ' + new_adc['id'])
            else:
                logging.info('Unable to create Ad Creactive.')

            logging.debug(f'Updating AdCreative of Ad..')
            res = ad.api_update(params={
                Ad.Field.creative: {'creative_id': new_adc[AdCreative.Field.id]}
            })

            logging.info(f'Update AdCreative ID success(id): ' + new_adc[AdCreative.Field.id])

            return res

    def create_ad_with_duplicate(self, path: str, name: str, job_num: int, template_id: str):
        # Check if video is already uploaded
        vid = self.is_video_uploaded(name)
        if vid is None:
            return False

        # Get uploaded video's info
        video = AdVideo(vid)
        video = video.api_get(fields=[AdVideo.Field.id, AdVideo.Field.title])
        v_name = video[AdVideo.Field.title]
        v_name = v_name.replace(".mp4", "")

        # Duplicate Campaign
        campaign = self.duplicate_campaign(template_id, job_num, self.get_adset_name_from_path(name, path))
        if campaign is None:
            return False

        # Duplicate ADSet
        ad_set = self.duplicate_adset(campaign, v_name)
        if ad_set is None:
            return False

        # Duplicate ADCreative and set uploaded video to it
        ad = self.duplicate_ad(ad_set, video, vid, v_name)
        if ad is None:
            return False

        return True

    def upload(self, path: str) -> Optional[UploadedVideo]:
        video = AdVideo(api=self._api)
        video._parent_id=self._act_id
        video[AdVideo.Field.filepath] = path
        res = video.remote_create()
        if res is not None and isinstance(res, dict) and 'id' in res:
            id = res['id']
            logging.info(f'Video created: id={id} res={res}')
            upl = UploadedVideo(id=res['id'])
            self._uploaded_videos[upl.id] = upl
            return upl
        else:
            raise Exception('unable to upload video')

    def set_uploaded_videos(self, files: List[UploadedVideo]):
        self._uploaded_videos = dict(zip(map(lambda x: x.id, files), files))

    def wait_all(self) -> Generator[Tuple[str,str], None, None]:
        sleepTime = 30
        retries = 10
        for i in range(0,retries):
            to_delete = []
            for id in self._uploaded_videos:
                video = self._uploaded_videos[id]
                self.reload(video)
                g = self.get_by_id(id)
                if g is not None:
                    yield id, g.status
                    if g.status == VIDEO_STATUS_READY:
                        to_delete.append(id)
            for id in to_delete:
                del self._uploaded_videos[id]
            if len(self._uploaded_videos) == 0:
                break
            logging.info(f'The are {len(self._uploaded_videos)} with not ready status. Going to sleep for {sleepTime} seconds. Try {i} of {retries}')
            time.sleep(sleepTime)