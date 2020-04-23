import logging
import os
import time
from http import HTTPStatus
from typing import List, Optional, Generator, Tuple

from facebook_business import FacebookAdsApi
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

    def index_template_adset_names(self, templates):
        raise NotImplementedError

    def upload(self, path: str) -> Optional[UploadedVideo]:
        raise NotImplementedError

    def get_uploaded_video(self, name: str):
        raise NotImplementedError

    def duplicate_campaign(self, job_num, template_id):
        raise NotImplementedError

    def duplicate_adset(self, campaign, adset_name):
        raise NotImplementedError

    def duplicate_ad(self, ad_set, uploaded_video, video_id, video_name):
        raise NotImplementedError

    def create_ad_with_duplicate(self, path: str, name: str, job_num: int, template_id : str):
        raise NotImplementedError

    def set_uploaded_videos(self, files: List[UploadedVideo], names):
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
        self._uploaded_names = {}
        self._campaigns = {}
        self._campaigns_index_by_names = {}
        self._ad_sets = {}
        self._ad_sets_index_by_names = {}
        self._ads = {}
        self._ads_index_by_names = {}
        self._template_adset_names = {}

    def _index_videos(self, videos: List[UploadedVideo]):
        self._index.update(dict(zip(list(map(lambda x: x.name, videos)), videos)))
        self._index_ids.update(dict(zip(list(map(lambda x: x.id, videos)), videos)))

    def _index_campaigns(self, campaigns: List[Campaign]):
        self._campaigns.update(dict(zip(list(map(lambda c: c[Campaign.Field.id], campaigns)), campaigns)))
        self._campaigns_index_by_names.update(dict(zip(list(map(lambda c: c[Campaign.Field.name], campaigns)), campaigns)))

    def _index_ad_sets(self, ad_sets: List[AdSet]):
        self._ad_sets.update(dict(zip(list(map(lambda s: s[AdSet.Field.id], ad_sets)), ad_sets)))
        self._ad_sets_index_by_names.update(dict(zip(list(map(lambda s: s[AdSet.Field.name], ad_sets)), ad_sets)))

    def _index_ads(self, ads: List[Ad]):
        self._ads.update(dict(zip(list(map(lambda a: a[Ad.Field.adset_id], ads)), ads)))
        self._ads_index_by_names.update(dict(zip(list(map(lambda a: a[Ad.Field.name], ads)), ads)))

    def _add_campaign_to_index(self, campaign: Campaign):
        self._campaigns.update({campaign[Campaign.Field.id]: campaign})
        self._campaigns_index_by_names.update({campaign[Campaign.Field.name]: campaign})

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

    def _resp_to_adobject(self, v):
        return v

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
        logging.info(f'starting advideo indexing for account={self._act_id} with limit={limit}')
        try:
            c = Cursor(self._act, AdVideo, fields=[AdVideo.Field.title,AdVideo.Field.status], params={"limit":limit})
            self._index_videos(list(map(self._resp_to_video2, c)))
            logging.info(f'Indexed {len(self._index_ids)} videos for account {self._act_id}')

            c = Cursor(self._act, Campaign, fields=[Campaign.Field.id, Campaign.Field.name])
            self._index_campaigns(list(map(self._resp_to_adobject, c)))
            logging.info(f'Indexed {len(self._campaigns)} campaigns for account {self._act_id}')

            c = Cursor(self._act, AdSet, fields=[AdSet.Field.id, AdSet.Field.name, AdSet.Field.campaign_id])
            self._index_ad_sets(list(map(self._resp_to_adobject, c)))
            logging.info(f'Indexed {len(self._ad_sets)} adsets for account {self._act_id}')

            c = Cursor(self._act, Ad, fields=[Ad.Field.id, Ad.Field.name, Ad.Field.adset_id])
            self._index_ads(list(map(self._resp_to_adobject, c)))
            logging.info(f'Indexed {len(self._ads)} ads for account {self._act_id}')

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

    def index_template_adset_names(self, templates):
        names = []
        for template_id in templates:
            if template_id != '':
                template_campaign = self._campaigns[template_id]
                ad_sets = template_campaign.get_ad_sets(fields=[
                    AdSet.Field.name,
                    AdSet.Field.id
                ])
                names.append({'campaign_id': template_campaign['id'], 'name': ad_sets[0]['name']})
        self._template_adset_names = dict(zip(map(lambda x: x['campaign_id'], names), names))

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

    def get_uploaded_video(self, name: str):
        video = self.get_by_name(name)
        if video is None:
            if name in self._uploaded_names:
                return self._uploaded_names[name]
        else:
            return video

        return None

    def duplicate_campaign(self, job_num, template_id):
        template = None
        if template_id in self._campaigns:
            template = self._campaigns[template_id]
        else:
            logging.warning(f'Template campaign does not exist: id={template_id}')

        campaign_name = template[Campaign.Field.name] + str(job_num)
        logging.debug(f'Checking campaign existence for: name={campaign_name}')
        campaign = None
        if campaign_name in self._campaigns_index_by_names:
            campaign = self._campaigns_index_by_names[campaign_name]

        if campaign is not None:
            logging.debug(f'Skip campaign duplicate: "name={campaign_name}"')
        else:
            cr = template.create_copy(fields=None, params={'deep_copy': True})
            campaign = Campaign(cr._data['copied_campaign_id'])
            campaign.api_update(params={Campaign.Field.name: campaign_name})
            campaign[Campaign.Field.name] = campaign_name
            if campaign is not None and isinstance(campaign, Campaign):
                self._add_campaign_to_index(campaign)
                logging.info(f'Duplicate campaign success: job_num={job_num} template={template_id}')
            else:
                logging.warning(f'Duplicate campaign failed: job_num={job_num} template={template_id}')
                return None

        return campaign

    def duplicate_adset(self, campaign, adset_name):
        logging.debug(f'Checking adset existence for: campaign_name={campaign[Campaign.Field.name]} name={adset_name}')
        ad_set = None
        if adset_name in self._ad_sets_index_by_names:
            ad_set = self._ad_sets_index_by_names[adset_name]

        if ad_set is not None and isinstance(ad_set, AdSet) and ad_set[AdSet.Field.campaign_id] == campaign[Campaign.Field.id]:
            logging.debug(f'Skip adset duplicate: name={adset_name}')
        else:
            ad_sets = campaign.get_ad_sets(fields=[
                AdSet.Field.name,
                AdSet.Field.id
            ])

            adsett = ad_sets[0]
            is_delete_adset = False
            if adsett['name'] == self._template_adset_names[self._template_campaign_id]['name']:
                is_delete_adset = True

            asr = adsett.create_copy(params={'deep_copy': True, 'campaign_id': campaign['id']})
            if is_delete_adset:
                res = adsett.api_delete()
                logging.info(f'Deleted template adSet: id={res[AdSet.Field.id]}')

            ad_set = AdSet(asr._data['copied_adset_id'])
            ad_set.api_update(params={AdSet.Field.name: adset_name})

            if ad_set is not None and isinstance(ad_set, AdSet):
                logging.info(f'AdSet Duplicate success: campaign={campaign[Campaign.Field.id]} name={adset_name}')
            else:
                logging.warning(f'Unable to create AdSet: campaign={campaign[Campaign.Field.id]} name={adset_name}')
                return None

        return ad_set

    def duplicate_ad(self, ad_set, uploaded_video, video_id, video_name):
        logging.debug(f'Checking ad existence for: ad_set_name={ad_set[AdSet.Field.id]} name={video_name}')
        ad = None
        if video_name in self._ads_index_by_names:
            ad = self._ads_index_by_names[video_name]
        if ad is not None and isinstance(ad, Ad) and ad[Ad.Field.adset_id] == ad_set[Ad.Field.id]:
            logging.debug(f'Skip Ad: name={video_name}')
            return True
        else:
            logging.debug(f'Ad Updating: "{video_name}"')
            ads = ad_set.get_ads(fields=[Ad.Field.id, Ad.Field.name, Ad.Field.creative])  # get again
            if not len(ads) == 1:
                logging.debug(f'Template Ad should only be one.')
                return None;
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
                logging.info(f'AdCreative create success: id={new_adc[AdCreative.Field.id]}')
            else:
                logging.warning(f'Unable to create Ad creactive: ad_set={ad_set[Ad.Field.adset_id]}, video_name={video_name}')

            res = ad.api_update(params={
                Ad.Field.creative: {'creative_id': new_adc[AdCreative.Field.id]}
            })

            logging.debug(f'Update ad_creative success: id={new_adc[AdCreative.Field.id]}')
            return res

    def create_ad_with_duplicate(self, path: str, name: str, job_num: int, template_id: str):
        # Check if video is already uploaded
        video = self.get_uploaded_video(name)
        if video is None:
            logging.warning(f'Video is not uploaded yet:"{name}"')
            return False

        # Get uploaded video's info
        v_id = video.id
        v_name = video.name
        v_name = v_name.replace(".mp4", "")
        video = AdVideo(v_id)
        video[Ad.Field.name] = v_name
        self._template_campaign_id = template_id

        # Duplicate Campaign
        logging.debug(f'Duplicating campaign for job_num:{job_num}, template:{template_id}')
        campaign = self.duplicate_campaign(job_num, template_id)
        if campaign is None:
            return False
        logging.debug(f"Successful: id={campaign[Campaign.Field.id]}")

        # Duplicate ADSet
        logging.debug(f'Duplicating adset for campaign:{campaign[Campaign.Field.id]}, name:{v_name}')
        ad_set = self.duplicate_adset(campaign, v_name)
        if ad_set is None:
            return False
        logging.debug(f"Successful: id={ad_set[AdSet.Field.id]}, job_num:{job_num}, template:{template_id}")

        # Duplicate ADCreative and set uploaded video to it
        logging.debug(f'Duplicating ad for adset:{ad_set[AdSet.Field.id]}, video:{v_name}')
        res = self.duplicate_ad(ad_set, video, v_id, v_name)
        logging.debug(f"Duplicate ad: res={res}, job_num:{job_num}, template:{template_id}")

        return res

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

    def set_uploaded_videos(self, files: List[UploadedVideo], names):
        self._uploaded_videos = dict(zip(map(lambda x: x.id, files), files))
        self._uploaded_names = dict(zip(map(lambda x: x.name, files), names))

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