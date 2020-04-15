from analytics.connectors._Utils import my_slice, create_fields
import requests, time


class VKApp:
    def __init__(self, access_token, account_id, client_id, client_name):
        self.__access_token = access_token
        self.__v = "5.101"
        self.__method_url = "https://api.vk.com/method/"
        self.account_id = account_id
        self.client_id = client_id

        self.report_dict = {
            "CAMPAIGNS": {
                "fields": {"id": "STRING", "type": "STRING", "name": "STRING",
                           "status": "STRING", "day_limit": "FLOAT", "all_limit": "FLOAT",
                           "start_time": "STRING", "stop_time": "STRING", "create_time": "STRING",
                           "update_time": "STRING", "views_limit": "STRING"}},
            "ADS": {
                "fields": {"id": "STRING", "campaign_id": "STRING", "status": "STRING", "approved": "STRING",
                           "create_time": "STRING", "update_time": "STRING", "goal_type": "STRING",
                           "cost_type": "STRING", "day_limit": "FLOAT", "all_limit": "FLOAT", "start_time": "STRING",
                           "stop_time": "STRING", "category1_id": "STRING", "category2_id": "STRING",
                           "age_restriction": "STRING", "name": "STRING", "events_retargeting_groups": "STRING",
                           "ad_format": "STRING", "cpc": "FLOAT", "ad_platform": "STRING", "cpm": "FLOAT",
                           "impressions_limit": "INTEGER", "ad_platform_no_ad_network": "STRING", "ocpm": "FLOAT",
                           "weekly_schedule_use_holidays": "STRING", "weekly_schedule_hours": "STRING",
                           "impressions_limited": "STRING", "ad_platform_no_wall": "STRING",
                           "autobidding_max_cost": "STRING", "autobidding": "STRING"}},

            "CAMPAIGN_STAT": {
                "fields": {"day": "STRING", "spent": "FLOAT", "impressions": "INTEGER", "clicks": "INTEGER",
                           "reach": "INTEGER", "join_rate": "INTEGER", "campaign_id": "STRING", 'lead_form_sends': "STRING",
                           'goals': "STRING"}},

            "ADS_STAT": {
                "fields": {"ad_id": "STRING", "clicks": "INTEGER", "day": "STRING", "impressions": "INTEGER",
                           "join_rate": "FLOAT", "reach": "INTEGER", "spent": "FLOAT", 'lead_form_sends': "INTEGER",
                           'goals': "INTEGER"}},
            "SEX_STAT": {
                "fields": {"impressions_rate": "FLOAT", "clicks_rate": "FLOAT", "value": "STRING",
                           "ad_id": "STRING", "day": "STRING"}},
            "AGE_STAT": {
                "fields": {"impressions_rate": "FLOAT", "clicks_rate": "FLOAT", "value": "STRING",
                           "ad_id": "STRING", "day": "STRING"}},
            "SEX_AGE_STAT": {
                "fields": {"impressions_rate": "FLOAT", "clicks_rate": "FLOAT", "value": "STRING",
                           "ad_id": "STRING", "day": "STRING"}},
            "CITIES_STAT": {
                "fields": {"impressions_rate": "FLOAT", "clicks_rate": "FLOAT", "value": "STRING",
                           "ad_id": "STRING", "day": "STRING", "name": "STRING"}}}
        self.tables_with_schema, self.string_fields, self.integer_fields, self.float_fields = \
            create_fields(client_name, "VKontakte", self.report_dict)

    def __request(self, method, request_type='get', **kwargs):
        if "access_token" not in kwargs:
            params = {'access_token': self.__access_token, 'v': self.__v}
            for key, value in kwargs.items():
                params[key] = value
        else:
            params = kwargs
        request_data = requests.request(request_type, self.__method_url + method, params=params)
        return self.__get_errors(request_data)

    def __get_errors(self, response):
        if response.status_code == 200:
            if 'error' in response.json():
                error_msg = response.json()['error']['error_msg']
                error_code = response.json()['error']['error_code']
                raise Exception(f"Error code: {error_code}. Error msg: {error_msg}")
            elif 'response' in response.json():
                return response.json()['response']
            else:
                raise Exception("Это что-то новое")

        else:
            raise Exception("Status code not 200", response.status_code, response.content)

    def get_campaigns(self):
        campaigns = self.__request('ads.getCampaigns', request_type='get', account_id=self.account_id,
                                   include_deleted=1, client_id=self.client_id)
        return campaigns

    def get_ads(self, campaign_ids):
        campaign_ids = my_slice(campaign_ids, 100)
        ads_list = []
        for campaign_ids_list in campaign_ids:
            campaign_ids_string = ",".join([str(x) for x in campaign_ids_list])
            ads = self.__request('ads.getAds', request_type='get', account_id=self.account_id,
                                 campaign_ids=f"[{campaign_ids_string}]", client_id=self.client_id)
            for ad in ads:
                ads_list.append(ad)
            time.sleep(2)
        return ads_list

    def get_demographics(self, demographics_list_ids, date_from, date_to, limit=2000):
        data_keys = {"sex": [], "age": [], "sex_age": [], "cities": []}
        demographics_list = my_slice(demographics_list_ids, limit)
        for demographics_id_list in demographics_list:
            demographics_ids_string = ",".join([str(x) for x in demographics_id_list])
            demographics_response = self.__request('ads.getDemographics', request_type='get',
                                                   account_id=self.account_id,
                                                   ids_type="ad", ids=demographics_ids_string, period="day",
                                                   date_from=date_from, date_to=date_to)

            for one in demographics_response:
                for element in one['stats']:
                    for key, value in element.items():
                        if key in ['sex', 'age', 'sex_age', 'cities']:
                            for one_element in value:
                                arr = one_element.copy()
                                arr[one['type'] + '_id'] = one['id']
                                arr['day'] = element['day']
                                data_keys[key].append(arr)

        return data_keys['sex'], data_keys['age'], data_keys['sex_age'], data_keys['cities']

    def get_day_stats(self, ids_type, list_of_ids, date_from, date_to, limit=2000):
        day_stat_list = []
        ids_list = my_slice(list_of_ids, limit)
        for ids_stat_list in ids_list:
            ids_stat_string = ",".join([str(x) for x in ids_stat_list])
            day_stats = self.__request('ads.getStatistics', request_type='get', account_id=self.account_id,
                                       ids_type=ids_type, ids=ids_stat_string, period="day", date_from=date_from,
                                       date_to=date_to)
            for DayStat in day_stats:
                for stat in DayStat['stats']:
                    stat[DayStat['type'] + "_id"] = DayStat['id']
                    day_stat_list.append(stat)
            time.sleep(2)
        return day_stat_list

