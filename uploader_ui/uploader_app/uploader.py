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

    def get_campaign_by_name(self, campaigns, name: str) -> bool:
        raise NotImplementedError

    def upload(self, path: str) -> Optional[UploadedVideo]:
        """
        Upload file by given path and return UploadedVideo
        :param path:
        :raise Exception if upload failed
        :return:
        """
        raise NotImplementedError

    def upload_to_campaign(self, path: str, name: str, job_num) -> bool:
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
                return True
        return False

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

    def upload_to_campaign(self, path: str, name: str, job_num) -> bool:
        video = AdVideo(api=self._api)
        video._parent_id = self._act_id
        video[AdVideo.Field.filepath] = path
        campaign_name = os.getenv('AD_CAMPAIGN_TEMPLATE_ID', 'US-AND-MAI-ABO-J')
        campaigns = self._act.get_campaigns(fields=[Campaign.Field.name, Campaign.Field.id])
        logging.info(f'Checking Campaign Exist: "{campaign_name}"')
        if self.get_campaign_by_name(campaigns, campaign_name):
            logging.info(f'Campaign is already exist: "{campaign_name}"')
        else:
            logging.info(f'Creating Campaign: "{campaign_name}"')
            params = {
                'name': campaign_name,
                'objective': 'POST_ENGAGEMENT',
                'special_ad_category' : 'NONE',
                'status': 'ACTIVE',
            }
            campaign_result = self._act.create_campaign(params=params)
            logging.info(campaign_result)

        # Todo. To Create Set of Campaign
        
        # Todo. To Create AD to Set


        # logging.info(f'Current Compaigns: {campaigns}');
        return False
        # video = AdVideo(api=self._api)
        # video._parent_id=self._act_id
        # video[AdVideo.Field.filepath] = path
        # res = video.remote_create()
        # if res is not None and isinstance(res, dict) and 'id' in res:
        #     id = res['id']
        #     logging.info(f'Video created: id={id} res={res}')
        #     upl = UploadedVideo(id=res['id'])
        #     self._uploaded_videos[upl.id] = upl
        #     return upl
        # else:
        #     raise Exception('unable to upload video')

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