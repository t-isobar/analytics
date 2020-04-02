import requests, json, sys
from datetime import datetime, timedelta
from analytics.connectors._Utils import create_fields


class Hybrid:
    def __init__(self, client_id, client_secret, client_name, path_to_json):
        self.client_id = client_id
        self.client_secret = client_secret
        self.client_name = client_name
        self.path_to_json = path_to_json
        self.url = "https://api.hybrid.ru/"
        
        self.report_dict = {
            "CAMPAIGNS": {"fields": {"Id": "STRING", "Name": "STRING"}},

            "CAMPAIGN_STAT": {
                "fields": {"Day": "STRING", "ImpressionCount": "INTEGER", "ClickCount": "INTEGER", "Reach": "INTEGER",
                       "CTR": "FLOAT", "id": "STRING"}},

            "ADVERTISER_STAT": {
                "fields": {"Day": "STRING", "ImpressionCount": "INTEGER", "ClickCount": "INTEGER", "Reach": "INTEGER",
                       "CTR": "FLOAT"}}
        }

        self.tables_with_schema, self.string_fields, self.integer_fields, self.float_fields = \
            create_fields(client_name, "Hybrid")
        
    def hybrid_auth(self, **kwargs):
        body = {
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret
                }
        body.update(kwargs)
        keys = requests.post(self.url + 'token', body).json()
        keys['token_life'] = datetime.timestamp(datetime.now() + timedelta(seconds=keys['expires_in']))
        json.dump(keys, open(self.path_to_json + f"{self.client_name}.json", "w"))
        return keys['access_token']
    
    def check_token_expires(self):
        keys = json.load(open(self.path_to_json + f"{self.client_name}.json", "r"))
        token_life_remaining = (datetime.fromtimestamp(keys['token_life']) - datetime.now()).seconds
        if token_life_remaining <= 60:
            access_token = self.hybrid_auth(refresh_token=keys['refresh_token'])
            return access_token
        else:
            return keys['access_token']
    
    def get_request(self, method, **kwargs):
        access_token = self.check_token_expires()
        headers = {"Authorization": f"Bearer {access_token}"}
        response = requests.get(self.url + method, headers=headers, params=kwargs).json()
        return response
    
    def get_campaigns(self):
        campaigns = self.get_request("v3.0/advertiser/campaigns")
        return campaigns
    
    def get_campaign_stat(self, campaign_ids_list, date_from, date_to):
        total_stat = []
        for ids in campaign_ids_list:
            params = {"from": date_from, "to": date_to, "campaignId": ids}
            stat = self.get_request("v3.0/campaign/Day", **params)
            if list(stat.keys()) != ['Statistic', 'Total']:
                sys.exit(list(stat.keys()))
            for element in stat['Statistic']:
                element['id'] = ids
                total_stat.append(element)
        return total_stat
    
    def get_advertiser_stat(self, date_from, date_to):
        params = {"from": date_from, "to": date_to}
        stat = self.get_request("v3.0/advertiser/Day", **params)
        if list(stat.keys()) != ['Statistic', 'Total']:
            sys.exit(list(stat.keys()))
        return stat['Statistic']
