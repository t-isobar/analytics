#!usr/bin/python3

import requests, time
import pandas as pd
import _BigQuery, _Utils


class MyTarget:
    def __init__(self, agency_access_token, client_name):
        self.agency_access_token = agency_access_token
        self.url = "https://target.my.com/"

        self.report_dict = {
            "ADS": {
                "fields": {"campaign_id": "STRING", "id": "STRING", "moderation_status": "STRING"}},

            "CAMPAIGNS": {
                "fields": {"id": "STRING", "name": "STRING", "package_id": "STRING"}},

            "BANNER_STAT": {
                "fields": {"clicks": "INTEGER","clicks_on_external_url": "INTEGER","comments": "INTEGER","cpa": "FLOAT",
                           "cpc": "FLOAT", "cpm": "FLOAT","cr": "FLOAT","ctr": "FLOAT","day": "STRING",
                           "depth_of_view": "INTEGER","fullscreen_off": "INTEGER","fullscreen_on": "INTEGER",
                           "goals": "INTEGER","id": "STRING","joinings": "INTEGER","launching_video": "INTEGER",
                           "likes": "INTEGER","measurement_rate": "FLOAT","moving_into_group": "INTEGER",
                           "opening_app": "INTEGER","opening_post": "INTEGER","paused": "INTEGER",
                           "resumed_after_pause": "INTEGER","sending_form": "INTEGER","shares": "INTEGER","shows": "INTEGER",
                           "sound_turned_off": "INTEGER","sound_turned_on": "INTEGER","spent": "FLOAT","started": "INTEGER",
                           "viewed_100_percent": "INTEGER","viewed_100_percent_cost": "FLOAT","viewed_100_percent_rate": "FLOAT",
                           "viewed_10_seconds": "INTEGER","viewed_10_seconds_cost": "FLOAT","viewed_10_seconds_rate": "FLOAT",
                           "viewed_25_percent": "INTEGER","viewed_25_percent_cost": "FLOAT","viewed_25_percent_rate": "FLOAT",
                           "viewed_50_percent": "INTEGER","viewed_50_percent_cost": "FLOAT","viewed_50_percent_rate": "FLOAT",
                           "viewed_75_percent": "INTEGER","viewed_75_percent_cost": "FLOAT","viewed_75_percent_rate": "FLOAT",
                           "votings": "INTEGER","vr": "FLOAT"}},

            "CAMPAIGN_STAT": {
                "fields": {"clicks": "INTEGER","clicks_on_external_url": "INTEGER","comments": "INTEGER","cpa": "FLOAT",
                           "cpc": "FLOAT", "cpm": "FLOAT","cr": "FLOAT","ctr": "FLOAT","day": "STRING",
                           "depth_of_view": "INTEGER","fullscreen_off": "INTEGER","fullscreen_on": "INTEGER",
                           "goals": "INTEGER","id": "STRING","joinings": "INTEGER","launching_video": "INTEGER",
                           "likes": "INTEGER","measurement_rate": "FLOAT","moving_into_group": "INTEGER",
                           "opening_app": "INTEGER","opening_post": "INTEGER","paused": "INTEGER",
                           "resumed_after_pause": "INTEGER","sending_form": "INTEGER","shares": "INTEGER","shows": "INTEGER",
                           "sound_turned_off": "INTEGER","sound_turned_on": "INTEGER","spent": "FLOAT","started": "INTEGER",
                           "viewed_100_percent": "INTEGER","viewed_100_percent_cost": "FLOAT","viewed_100_percent_rate": "FLOAT",
                           "viewed_10_seconds": "INTEGER","viewed_10_seconds_cost": "FLOAT","viewed_10_seconds_rate": "FLOAT",
                           "viewed_25_percent": "INTEGER","viewed_25_percent_cost": "FLOAT","viewed_25_percent_rate": "FLOAT",
                           "viewed_50_percent": "INTEGER","viewed_50_percent_cost": "FLOAT","viewed_50_percent_rate": "FLOAT",
                           "viewed_75_percent": "INTEGER","viewed_75_percent_cost": "FLOAT","viewed_75_percent_rate": "FLOAT",
                           "votings": "INTEGER","vr": "FLOAT"}}
        }

        self.tables_with_schema = {f"{client_name}_MyTarget_{report_name}": self.report_dict[report_name]['fields'] for report_name in list(self.report_dict.keys())}

        self.string_fields = list(set([key for values in self.report_dict.values() for key, value in values['fields'].items() if value == "STRING"]))
        self.integer_fields = list(set([key for values in self.report_dict.values() for key, value in values['fields'].items() if value == "INTEGER"]))
        self.float_fields = list(set([key for values in self.report_dict.values() for key, value in values['fields'].items() if value == "FLOAT"]))

    def request(self, method, offset=0, limit=25, result_list=[], **kwargs):
        headers = {"Authorization": "Bearer " + self.agency_access_token}
        params = {"offset": offset, "limit":limit}
        params.update(kwargs)
        request_data = requests.get(self.url+f"api/v2/{method}.json", headers=headers, params=params)
        if request_data != []:
            for element in request_data.json()['items']:
                result_list.append(element)
        count = request_data.json()['count']
        if offset <= count:
            time.sleep(1)
            offset += limit
            params.clear()
            return self.request(method, offset, limit, result_list=result_list, **kwargs)
        else:
            return result_list

    def get_banner_stat(self, date_from, date_to):
        headers = {"Authorization": "Bearer " + self.agency_access_token}
        params = {"date_from": date_from, "date_to": date_to, "metrics": "base,events,video"}
        stat = requests.get(self.url + "api/v2/statistics/banners/day.json", headers=headers, params=params).json()
        stat = self.prepare_stat(stat['items'])
        return stat

    def prepare_stat(self, stat):
        list_of_stat = []
        for one in stat:
            for element in one['rows']:
                stats = {}
                stats['id'] = one['id']
                stats['day'] = element['date']
                stats.update(element['base'])
                stats.update(element['video'])
                stats.update(element['events'])
                list_of_stat.append(stats)
        return list_of_stat

    def get_campaigns_stat(self, date_from, date_to):
        headers = {"Authorization": "Bearer " + self.agency_access_token}
        params = {"date_from": date_from, "date_to": date_to, "metrics": "base,events,video"}
        stat = requests.get(self.url + "api/v2/statistics/campaigns/day.json", headers=headers, params=params).json()
        stat = self.prepare_stat(stat['items'])
        return stat

    def get_campaigns(self, **kwargs):

        """

        _status - статус камании ("active", "blocked", "deleted")
        _status__ne - все кроме определнного статуса кампании (например: все, кроме активных)
        _status__in - любой из перечисленных через запятую статусов объявления

        _updated__gt - datetime последнего обновления кампании ("gt" - больше)
        _updated__gte - datetime последнего обновления кампании ("gte" - больше или равно)
        _updated__lt - datetime последнего обновления кампании ("lt" - меньше)
        _updated__lte - datetime последнего обновления кампании ("lte" - меньше или равно)

        """

        campaigns = self.request("campaigns", offset=0, limit=25, result_list=[], **kwargs)
        return campaigns

    def get_ads(self, **kwargs):

        """
        _campaign_id - id кампании
        _campaign_id__in - несколько id кампаний через запятую

        _campaign_status - статус кампании ("active", "blocked", "deleted")
        _campaign_status__ne - все кроме определнного статуса кампании (например: все, кроме активных)
        _campaign_status__in - любой из перечисленных через запятую статусов кампании

        _status - статус объявления ("active", "blocked", "deleted")
        _status__ne - все кроме определнного статуса объявления (например: все, кроме активных)
        _status__in - любой из перечисленных через запятую статусов объявления

        _updated__gt - datetime последнего обновления объявления ("gt" - больше)
        _updated__gte - datetime последнего обновления объявления ("gte" - больше или равно)
        _updated__lt - datetime последнего обновления объявления ("lt" - меньше)
        _updated__lte - datetime последнего обновления объявления ("lte" - меньше или равно)

        """

        ads = self.request("banners", offset=0, limit=50, result_list=[], **kwargs)
        return ads


def getMyTargetReport(access_token, client_name, client_login, path_to_bq, date_from, date_to, period):
	ut = _Utils.Utils()
	bq = _BigQuery.BigQuery(path_to_bq)
	mt = MyTarget(access_token, client_name)

	date_range = ut.slice_date_on_period(date_from, date_to, period)

	dataset_ID = f"{client_name}_MyTarget_{client_login}"

	bq.check_or_create_dataset(dataset_ID)
	bq.check_or_create_tables(mt.tables_with_schema, dataset_ID)

	campaigns = mt.get_campaigns()
	campaigns_df = pd.DataFrame(campaigns)
	bq.insert_difference(campaigns_df, mt.integer_fields, mt.float_fields, mt.string_fields, dataset_ID, f"{client_name}_MyTarget_CAMPAIGNS", 'id', 'id')

	ads = mt.get_ads()
	ads_df = pd.DataFrame(ads)
	bq.insert_difference(ads_df, mt.integer_fields, mt.float_fields, mt.string_fields, dataset_ID, f"{client_name}_MyTarget_ADS", 'id', 'id')

	for date_from, date_to in date_range:
	    stat_banner = mt.get_banner_stat(date_from, date_to)
	    if stat_banner == []:
	    	continue
	    stat_banner_df = pd.DataFrame(stat_banner)
	    bq.data_to_insert(stat_banner_df, mt.integer_fields, mt.float_fields, mt.string_fields, dataset_ID, f"{client_name}_MyTarget_BANNER_STAT")

	    stat_campaigns = mt.get_campaigns_stat(date_from, date_to)
	    stat_campaigns_df = pd.DataFrame(stat_campaigns)
	    bq.data_to_insert(stat_campaigns_df, mt.integer_fields, mt.float_fields, mt.string_fields, dataset_ID, f"{client_name}_MyTarget_CAMPAIGN_STAT")