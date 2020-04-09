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
    #'506640253392515',
    #'485759768730516'
]

FacebookAdsApi.init('2389713357935376', fbappkey, fbtoken)
fbact = AdAccount('act_659750741197329')

template = Campaign('23844416049080002')
template = template.api_get(fields=[Campaign.Field.name])
print(template)
cr = template.create_copy(fields=None, params={'deep_copy': True})

c = Campaign(cr._data['copied_campaign_id'])
c = c.api_get(fields=[Campaign.Field.id, Campaign.Field.name])
cname = template[Campaign.Field.name] + job
c.api_update(params={Campaign.Field.name: cname})

adsets = c.get_ad_sets()
adsett = adsets[0]

for vid in videos:
    # ids and names will already be indexed, but for the simplicity of use
    # getting it here
    video = AdVideo(vid) 
    video = video.api_get(fields=[AdVideo.Field.id,AdVideo.Field.title])
    vname = video[AdVideo.Field.title]
    vname = vname.replace(".mp4", "")
    print(vid)
    print(vname)

    # duplicate adset
    asr = adsett.create_copy(params={'deep_copy': True})

    adset = AdSet(asr._data['copied_adset_id'])
    adset.api_update(params={AdSet.Field.name: vname})  # rename adset

    # adset ads
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

    # this is an attempt at creating a copy
    # though full copy may be much more complex
    new_adc = fbact.create_ad_creative(params={
        'name':vname,
        'object_story_spec':spec
        })
    
    # using a new creative to update the ad
    ad.api_update(params={
        Ad.Field.creative: {'creative_id': new_adc[AdCreative.Field.id]}
    })

    # and it works
print("Finish!")