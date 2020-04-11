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

    def get_campaign_by_name(self, campaigns, name: str):
        raise NotImplementedError

    def get_adset_by_name(self, ads, name: str):
        raise NotImplementedError

    def get_adset_name_from_path(self, name:str, path:str):
        raise NotImplementedError

    def get_campaign_name_from_path(self, name:str, path:str):
        raise NotImplementedError

    def upload(self, path: str) -> Optional[UploadedVideo]:
        """
        Upload file by given path and return UploadedVideo
        :param path:
        :raise Exception if upload failed
        :return:
        """
        raise NotImplementedError

    def upload_with_duplicate(self, path: str, name: str, job_num, new_path) -> Optional[UploadedVideo]:
        """
        Upload file by given path to Facebook Campaign and return True when succeed
        :param path:
        :param name:
        :param job_num:
        :return:
        """
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

    def get_campaign_by_name(self, campaigns, name: str):
        for campaign in campaigns:
            if campaign["name"] == name:
                return campaign
        return None

    def get_adset_by_name(selfself, ads, name: str):
        for ad in ads:
            if ad['name'] == name:
                return ad
        return None

    def get_adset_name_from_path(self, name:str, path:str):
        file_path = path.replace('/' + name, "")
        file_path = file_path[file_path.rfind('/') + 1:]
        file_path = file_path[file_path.find('_') + 1:]
        return file_path
        #return file_path[file_path.rfind('/') + 1:]

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

    def upload_with_duplicate(self, name: str, path: str, job_num, new_path) -> Optional[UploadedVideo]:
        template = Campaign(os.getenv('AD_CAMPAIGN_TEMPLATE_ID', '23844416049080002'))

        template = template.api_get(fields=[Campaign.Field.name])
        campaign_name = template[Campaign.Field.name] + str(job_num) + "_" + self.get_adset_name_from_path(name, path)

        logging.info(f'Checking Campaign Exist: "{campaign_name}"')
        campaigns = self._act.get_campaigns(fields=[
            Campaign.Field.name,
            Campaign.Field.id
        ])
        campaign = self.get_campaign_by_name(campaigns, campaign_name)
        if campaign is not None:
            logging.info(f'Campaign is already exist: "{campaign_name}"')
        else:
            logging.info(f'Duplicating Campaign: "{campaign_name}"')
            cr = template.create_copy(fields=None, params={'deep_copy': True})
            campaign = Campaign(cr._data['copied_campaign_id'])
            campaign = campaign.api_get(fields=[Campaign.Field.id, Campaign.Field.name])
            campaign.api_update(params={Campaign.Field.name: campaign_name})

        # check campaign is valid
        if campaign is not None and isinstance(campaign, Campaign):
            logging.info(f'Campaign Duplicate success: "{campaign_name}"')
        else:
            raise Exception('Unable to duplicate campaign')

        adsets = campaign.get_ad_sets()
        adsett = adsets[0]

        # To upload video file to AdSet
        logging.info(f'Uploading video file:"{new_path}"')
        video = AdVideo(api=self._api)
        video._parent_id = self._act_id
        video[AdVideo.Field.filepath] = new_path
        res = video.remote_create()
        vid = video.get_id()
        logging.info(f'Video is created(id):"{vid}"')

        video = AdVideo(vid) # Not Need
        video = video.api_get(fields=[AdVideo.Field.id, AdVideo.Field.title])
        vname = video[AdVideo.Field.title]
        vname = vname.replace(".mp4", "")

        # duplicate adset
        logging.info(f'Duplicating AdSet: "{vname}"')
        asr = adsett.create_copy(params={'deep_copy': True})
        adset = AdSet(asr._data['copied_adset_id'])
        adset.api_update(params={AdSet.Field.name: vname})  # rename adset

        if adset is not None and isinstance(adset, AdSet):
            logging.info(f'Duplicate AdSet success: "{vname}"')
        else:
            raise Exception('unable to create AdSet')

        # adset ads
        logging.info(f'Creating AdCreative..')
        ads = adset.get_ads(fields=[Ad.Field.id, Ad.Field.name, Ad.Field.creative])
        ad = ads[0]  # should only be one ad
        ad[Ad.Field.name] = vname
        ad.api_update(params={Ad.Field.name: vname})  # rename ad

        adc = AdCreative(ad[Ad.Field.creative][AdCreative.Field.id])
        adc = adc.api_get(fields=[AdCreative.Field.object_story_spec])

        spec = adc[AdCreative.Field.object_story_spec]
        spec['video_data']['video_id'] = vid
        spec['video_data']['image_url'] = video.get_thumbnails(fields=[], params={})[0]['uri']
        spec['video_data'].pop('image_hash')

        new_adc = self._act.create_ad_creative(params={
            'name': vname,
            'object_story_spec': spec
        })
        if new_adc is not None and isinstance(new_adc, AdCreative):
            logging.info(f'AdCreative creation success(id): ' + new_adc['id'])
        else:
            raise Exception('Unable to create Ad Creactive.')

        # using a new creative to update the ad
        ad.api_update(params={
            Ad.Field.creative: {'creative_id': new_adc[AdCreative.Field.id]}
        })

        logging.info(f'Update AdCreative ID success(id): ' + new_adc[AdCreative.Field.id])

        if res is not None and isinstance(res, dict) and 'id' in res:
            id = res['id']
            logging.info(f'Video is created: id={id} res={res}')
            upl = UploadedVideo(id=res['id'])
            logging.info(f'Video is uploaded: id={id} res={res}')
            self._uploaded_videos[upl.id] = upl
            return upl
        else:
            raise Exception('Unable to upload video')

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