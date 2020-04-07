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

FacebookAdsApi.init('2389713357935376', fbappkey, fbtoken)
fbact = AdAccount('act_659750741197329')

template = Campaign('23844416049080002')
template = template.api_get(fields=[Campaign.Field.name])

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
    print(video.get_thumbnails(fields=[], params={}))
    video = video.api_get(fields=[AdVideo.Field.id, AdVideo.Field.title])
    vname = video[AdVideo.Field.title]
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
        AdCreative.Field.url_tags
    ]
    #adcreatives = fbact.get_ad_creatives(fields=fields, params={AdCreative.Field.id: "adc['id']"}) # 'ad_label_ids' also not
    #adcreative = adcreatives[0]

    adcreatives = fbact.get_ad_creatives(fields=fields)
    adcreative = adc
    for c in adcreatives:
         if c['id'] == adc['id']:
             adcreative = c

    print(adcreative)
    params = {
        "account_id": adcreative['account_id'],
        "actor_id": adcreative['actor_id'],
        "instagram_actor_id": adcreative['instagram_actor_id'],
        "instagram_permalink_url": adcreative['instagram_permalink_url'],
        "name": vname,
        "video_id": vid,
        "object_type": adcreative['object_type'],  # 'SPONSORED_VIDEO' or 'VIDEO'
        "body": adcreative['body'],
        "effective_authorization_category": adcreative['effective_authorization_category'],
        "effective_instagram_media_id": adcreative['effective_instagram_media_id'],
        "effective_instagram_story_id": adcreative['effective_instagram_story_id'],
        "effective_object_story_id": adcreative['effective_object_story_id'],
        "object_story_spec": {
            "instagram_actor_id": adcreative['instagram_actor_id'],
            "page_id": adcreative['object_story_spec']['page_id'],
            # "link_data": {
            #     'link': adcreative['object_story_spec']['video_data']['call_to_action']['value']['link'],
            #     'message': adcreative['object_story_spec']['video_data']['message'],
            #     "call_to_action": {
            #         "type": adcreative['object_story_spec']['video_data']['call_to_action']['type'],
            #         "value": {
            #             "link": adcreative['object_story_spec']['video_data']['call_to_action']['value']['link']
            #         }
            #     }
            # },
            "video_data":{
                'link_description': 'to work',
                'image_url': video.get_thumbnails(fields=[], params={})[0]['uri'],
                "video_id": vid,
                "call_to_action": {
                    "type": adcreative['object_story_spec']['video_data']['call_to_action']['type'],
                    "value": {
                        "link": adcreative['object_story_spec']['video_data']['call_to_action']['value']['link']
                    }
                }
            }
        }
    }
    new_adc = fbact.create_ad_creative(params=params)

    # using a new creative to update the ad
    ad.api_update(params={
        Ad.Field.creative: {'creative_id': new_adc[AdCreative.Field.id]}
    })

    # and it works
print("Finish!")