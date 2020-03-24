from datetime import datetime, timedelta
import requests, json, time
from connectors._Utils import create_fields, my_slice


class Facebook:
    def __init__(self, token, account, client_name):
        self.token = token
        self.account = account

        self.report_dict = {
            "CAMPAIGNS": {
                "fields": {"name": "STRING", "updated_time": "STRING", "created_time": "STRING",
                           "configured_status": "STRING", "effective_status": "STRING", "start_time": "STRING",
                           "stop_time": "STRING", "id": "STRING"}},
            "ADS_STAT": {
                "fields": {"clicks": "INTEGER", "impressions": "INTEGER", "spend": "FLOAT",
                           "video_p100_watched_actions": "INTEGER", "video_p25_watched_actions": "INTEGER",
                           "video_p50_watched_actions": "INTEGER", "video_30_sec_watched_actions": "INTEGER",
                           "video_p75_watched_actions": "INTEGER", "video_p95_watched_actions": "INTEGER",
                           "video_thruplay_watched_actions": "INTEGER", "video_avg_time_watched_actions": "FLOAT",
                           "conversions": "INTEGER", "ad_id": "STRING", "ad_name": "STRING", "campaign_id": "STRING",
                           "campaign_name": "STRING", "adset_id": "STRING", "adset_name": "STRING",
                           "date_start": "STRING", "date_stop": "STRING"}},
            "CAMPAIGN_STAT": {
                "fields": {"clicks": "INTEGER", "impressions": "INTEGER", "spend": "FLOAT", "date_start": "STRING",
                           "date_stop": "STRING", "campaign_id": "STRING", "campaign_name": "STRING",
                           "video_p100_watched_actions": "INTEGER", "video_p25_watched_actions": "INTEGER",
                           "video_p50_watched_actions": "INTEGER", "video_30_sec_watched_actions": "INTEGER",
                           "video_p75_watched_actions": "INTEGER", "video_p95_watched_actions": "INTEGER",
                           "video_thruplay_watched_actions": "INTEGER", "video_avg_time_watched_actions": "FLOAT",
                           "conversions": "INTEGER"}},
            "ADS_BASIC": {
                "fields": {"adset_id": "STRING", "campaign_id": "STRING", "id": "STRING", "name": "STRING",
                           "status": "STRING", "updated_time": "STRING"}},
            "ADS_LAYOUT": {
                "fields": {"page_id": "STRING", "instagram_actor_id": "STRING", "video_id": "STRING",
                           "message": "STRING", "call_to_action_type": "STRING", "link": "STRING",
                           "link_format": "STRING", "attachment_style": "STRING", "image_hash": "STRING",
                           "link_caption": "STRING", "id": "STRING", "caption": "STRING", "ad_id": "STRING"}},
            "ADSETS": {
                "fields": {"campaign_id": "STRING", "id": "STRING", "name": "STRING", "status": "STRING",
                           "start_time": "STRING", "updated_time": "STRING", "created_time": "STRING",
                           "end_time": "STRING"}}}

        self.tables_with_schema, self.string_fields, self.integer_fields, self.float_fields = \
            create_fields(client_name, "Facebook")

    def prepare_data(self, list_of_data_lists):
        one_list = []
        for one in list_of_data_lists:
            for element in one:
                for data_one in element:
                    one_list.append(data_one)
        return one_list

    def check_headers(self, headers):
        if (headers['total_cputime'] >= 50) or (headers['total_time'] >= 50):
            print("Пришло время для сна.")
            time.sleep(60*60)

    def expand_dict(self, data_to_expand, dict_with_keys, dict_with_data):
        for key, value in data_to_expand.items():
            if isinstance(value, dict):
                dict_with_data = self.expand_dict(value, dict_with_keys, dict_with_data)
            elif isinstance(value, list):
                for element_of_list in value:
                    dict_with_data = self.expand_dict(element_of_list, dict_with_keys, dict_with_data)
            else:
                if key in dict_with_keys.keys():
                    dict_with_data[dict_with_keys[key]] = value
                else:
                    if key == 'link':
                        dict_with_data.setdefault(key, []).append(value)
                    else:
                        dict_with_data[key] = value

        return dict_with_data

    def get_ads_or_adsets(self, campaign_ids, method):
        campaign_ids_slice = my_slice(campaign_ids, 50, [])
        campaign_ids_2_slice = my_slice(campaign_ids_slice, 50, [])
        fields_dict = {
            "ads-basic": {
                "fields": "name,id,status,adset_id,updated_time,campaign_id", "limit": 500,  "dict_of_keys":
                    {"type": "call_to_action_type"}, "method": "ads"},
            "ads-layout": {
                "fields": "adcreatives{object_story_spec}", "limit": 200, "dict_of_keys":
                    {"type": "call_to_action_type"}, "method": "ads"},
            "adsets": {
                "fields": "campaign_id,id,name,status,start_time,updated_time,created_time,end_time", "limit": 500,
                "dict_of_keys": {}, "method": "adsets"}}

        get_data_params = fields_dict.get(method, False)

        if get_data_params:
            result = self.get_data_with_filtering(campaign_ids_2_slice, get_data_params["method"],
                                                  get_data_params['fields'], "campaign.id", 'IN',
                                                  get_data_params['limit'])
            result_data = []

            for one_element_in_list in result:
                if method == 'ads-layout':
                    mid_result_data = self.expand_dict(one_element_in_list['adcreatives'],
                                                       get_data_params['dict_of_keys'], {})
                    mid_result_data['ad_id'] = one_element_in_list['id']
                    result_data.append(mid_result_data)
                else:
                    result_data.append(self.expand_dict(one_element_in_list, get_data_params['dict_of_keys'], {}))
            return result_data
        else:
            return "Указан недопустимый метод."

    def get_campaigns(self, campaigns_list=[], after=''):
        fields = "name,updated_time,created_time,configured_status,effective_status,start_time,stop_time"
        params = {"access_token": self.token, "limit": 500, "fields": fields}
        if after != '':
            params['after'] = after
        campaigns = requests.get("https://graph.facebook.com/v6.0/"+self.account+"/campaigns", params=params)
        for element in campaigns.json()['data']:
            campaigns_list.append(element)
        if "next" in campaigns.json()['paging']:
            return self.get_campaigns(campaigns_list, after=campaigns.json()['paging']['cursors']['after'])
        return campaigns_list, json.loads(campaigns.headers['x-business-use-case-usage'])[self.account[4:]][0]

    def get_ads_or_campaign_statistics(self, date_from, date_to, level):
        total_result = []
        url_method = f"v6.0/{self.account}/insights?"

        date_from_dt = datetime.strptime(date_from, "%Y-%m-%d")
        date_to_dt = datetime.strptime(date_to, "%Y-%m-%d")
        list_of_days = [datetime.strftime(date_from_dt + timedelta(days=x), "%Y-%m-%d") for x in
                        range(0, (date_to_dt - date_from_dt).days + 1)]
        list_of_days_slice = my_slice(list_of_days, 50, [])

        dict_of_params = {
            "ad": {
                "fields": "clicks,impressions,spend,video_p100_watched_actions,video_p25_watched_actions,"
                          "video_p50_watched_actions,video_30_sec_watched_actions,video_p75_watched_actions,"
                          "video_p95_watched_actions,video_thruplay_watched_actions,video_avg_time_watched_actions,"
                          "conversions,ad_id,ad_name,campaign_id,campaign_name,adset_id,adset_name", "limit": 50},
            "campaign": {
                "fields": "impressions,clicks,spend,campaign_id,campaign_name,video_p100_watched_actions,"
                          "video_p25_watched_actions,video_p50_watched_actions,video_30_sec_watched_actions,"
                          "video_p75_watched_actions,video_p95_watched_actions,video_thruplay_watched_actions,"
                          "video_avg_time_watched_actions,conversions", "limit": 100}}

        get_data_params = dict_of_params.get(level, False)
        if get_data_params:
            count_list_of_days_slice = len(list_of_days_slice)
            for num, days in enumerate(list_of_days_slice, 1):
                batch = []
                for day in days:
                    time_range = "{'since':'%s','until':'%s'}" % (day, day)
                    batch.append(self.create_betch(url_method, time_range=time_range, fields=get_data_params['fields'],
                                                   limit=get_data_params['limit'], level=level, time_increment=1))

                result_list, headers = self.get_batch_data(batch, [], url_method)
                for one in result_list:
                    for key, value in one.items():
                        if "video_" in key:
                            one[key] = value[0]['value']
                    total_result.append(one)
                if num != count_list_of_days_slice:
                    self.check_headers(headers)
            return total_result

        else:
            return "Указан недопустимый уровень."

    def get_paging_data(self, result_data, headers):
        my_list = []
        result_paging = []
        for result in result_data:
            middle_data = json.loads(result['body'])
            for header in result['headers']:
                if header['name'] == 'X-Business-Use-Case-Usage':
                    headers = json.loads(header['value'])[self.account[4:]][0]
            md = middle_data['data']
            if md != []:
                for element in middle_data['data']:
                    my_list.append(element)
                result_paging.append(middle_data['paging'])
            else:
                continue

        return my_list, result_paging, headers

    def create_next_paging_request(self, result_paging, url_method):
        next_pages_batch = []
        for next_link in result_paging:
            if "next" in next_link:
                next_params = next_link['next'].split('?')[1]
                next_pages_batch.append({"method": "GET", "relative_url": url_method + next_params})
        return next_pages_batch

    def get_batch_request(self, batch):
        jBatch = json.dumps(batch)
        params = {"access_token": self.token, "batch": jBatch}
        result_data = requests.post("https://graph.facebook.com/", params=params).json()
        return result_data

    def get_data_with_filtering(self, campaign_ids_slice, method, fields, filter_by, operator, limit=500):
        result_list = []
        total_result = []
        batch = []
        url_method = f"v6.0/{self.account}/{method}?"
        count_campaign_ids_slice = len(campaign_ids_slice)
        for num, ids_slice in enumerate(campaign_ids_slice, 1):
            for ids in ids_slice:
                filtering = "[{field:'%s',operator:'%s',value:%s}]" % (filter_by, operator, ids)
                batch.append(self.create_betch(url_method, filtering=filtering, limit=limit, fields=fields))

            result_list, headers = self.get_batch_data(batch, result_list, url_method)
            print(headers)
            for one in result_list:
                total_result.append(one)
            if num != count_campaign_ids_slice:
                self.check_headers(headers)

        return total_result

    def create_betch(self, url_method, **kwargs):
        params = []
        for key, value in kwargs.items():
            params.append(f'{key}={value}')
        params_in_string = '&'.join(params)
        return {"method": "GET", "relative_url": url_method + params_in_string}

    def get_batch_data(self, batch, result_list, url_method):
        jBatch = json.dumps(batch)
        params = {"access_token": self.token, "batch": jBatch}
        result_data = requests.post("https://graph.facebook.com/", params=params).json()
        middle_list, result_paging, headers = self.get_paging_data(result_data, headers={})
        for element in middle_list:
            result_list.append(element)
        if result_paging != []:
            result_list, headers = self.get_other_data(result_list, result_paging, url_method, headers)
        return result_list, headers

    def get_data_no_filtering(self, method, **kwargs):
        result_list = []
        url_method = f"v6.0/{self.account}/{method}?"
        batch = [self.create_betch(url_method, **kwargs)]
        result_list = self.get_batch_data(batch, result_list, url_method)
        return result_list

    def get_other_data(self, result_list, result_paging, url_method, headers={}):
        next_pages_batch = self.create_next_paging_request(result_paging, url_method)
        result_data = self.get_batch_request(next_pages_batch)
        middle_list, result_paging, headers = self.get_paging_data(result_data, headers)
        for element in middle_list:
            result_list.append(element)
        if result_paging != []:
            return self.get_other_data(result_list, result_paging, url_method, headers)
        return result_list, headers
