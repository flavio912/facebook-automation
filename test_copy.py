from facebook_business import FacebookAdsApi
from facebook_business.api import Cursor
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.advideo import AdVideo
from facebook_business.adobjects.ad import Ad
from facebook_business.adobjects.adset import AdSet
from facebook_business.adobjects.campaign import Campaign
from facebook_business.adobjects.adcreative import AdCreative
import os

fbtoken = os.getenv('FB_GA_TOKEN')
fbappkey = os.getenv('FB_GA_APPKEY')

job = '999_Test_Job'
videos = [
    '2926386880739508',
    '506640253392515',
    '485759768730516'
]

FacebookAdsApi.init('2389713357935376',fbappkey, fbtoken)
fbact = AdAccount('act_659750741197329')

template = Campaign('23844416049080002')
template = template.api_get(fields=[Campaign.Field.name])

cr = template.create_copy(fields=None, params={'deep_copy':True})

c = Campaign(cr._data['copied_campaign_id'])
c = c.api_get(fields=[Campaign.Field.id,Campaign.Field.name])
cname = template[Campaign.Field.name] + job
c.api_update(params={Campaign.Field.name:cname})

adsets = c.get_ad_sets()
adsett = adsets[0]

for vid in videos:
    # ids and names will already be indexed, but for the simplicity of use 
    # getting it here
    video = AdVideo(vid) 
    video = video.api_get(fields=[AdVideo.Field.id,AdVideo.Field.title])
    vname = video[AdVideo.Field.title]
    print(vid)
    print(vname)

    # duplicate adset
    asr = adsett.create_copy(params={'deep_copy':True})

    adset = AdSet(asr._data['copied_adset_id'])
    adset.api_update(params={AdSet.Field.name:vname}) # rename adset

    #adset ads
    ads = adset.get_ads(fields=[Ad.Field.id,Ad.Field.name,Ad.Field.creative])

    ad = ads[0] # should only be one ad

    ad[Ad.Field.name] = vname
    ad.api_update(params={Ad.Field.name:vname}) # rename ad

    adc = AdCreative(ad[Ad.Field.creative][AdCreative.Field.id])
    adc = adc.api_get(fields=[AdCreative.Field.object_story_spec])

    # adc = adc.api_get(fields=[
    # AdCreative.Field.video_id,
    # AdCreative.Field.call_to_action_type,
    # AdCreative.Field.body,
    # AdCreative.Field.object_type,
    # AdCreative.Field.object_story_spec
    # ])
    # print(adc)

    # AdCreative contents is somewhat like:
    # <AdCreative> {
    #     "body": "\"I play every morning. <3 <3 <3 this app!\" - Parker C.",
    #     "call_to_action_type": "PLAY_GAME",
    #     "id": "23844463195810002",
    #     "object_story_spec": {
    #         "instagram_actor_id": "677652652254752",
    #         "page_id": "628994917168628",
    #         "video_data": {
    #             "call_to_action": {
    #                 "type": "PLAY_GAME",
    #                 "value": {
    #                     "link": "http://play.google.com/store/apps/details?id=com.luckyday.app"
    #                 }
    #             },
    #             "image_hash": "d3d8e456c3420d0ca7ef545925e67770",
    #             "message": "\"I play every morning. <3 <3 <3 this app!\" - Parker C.",
    #             "title": "Free Lotto for LIFE!",
    #             "video_id": "2445391002378618"
    #         }
    #     },
    #     "object_type": "VIDEO",
    #     "video_id": "2453107751606943"
    # }

    spec = adc[AdCreative.Field.object_story_spec]
    spec['video_data']['video_id'] = vid
    # image hash is not replaced, so it is still thumbnail from the original video.
    # however this is close enough to a workable solution
    
    # this is an attempt at creating a copy
    # though full copy may be much more complex
    new_adc = fbact.create_ad_creative(params={
        'name':vname,
        'object_story_spec':spec
        })
    
    # using a new creative to update the ad
    ad.api_update(params={
        Ad.Field.creative:{'creative_id':new_adc[AdCreative.Field.id]}
    })

    # and it works
