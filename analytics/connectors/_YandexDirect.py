import json, requests
from analytics.connectors._Utils import expand_dict, create_fields, my_slice


class YandexDirect:
    def __init__(self, access_token, client_name, client_login=""):
        self.url = "https://api.direct.yandex.com/json/v5/"
        self.client_login = client_login
        self.client_name = client_name
        self.headers_report = {
            "Authorization": "Bearer " + access_token,
            "Accept-Language": "ru"}

        self.report_dict = {
            "CLIENTS": {
                "fields": {
                    'ClientId': "STRING", 'Login': "STRING", 'ClientInfo': "STRING"
                }
            },
            "CAMPAIGNS": {
                "fields": {
                    "Name": "STRING", "Id": "STRING", "Type": "STRING"
                }
            },
            "ADGROUPS": {
                "fields": {
                    "Id": "STRING", "Name": "STRING", "CampaignId": "STRING", "Type": "STRING"
                }
            },
            "ADS": {
                "fields": {
                    "Type": "STRING", "Text": "STRING", "Title": "STRING", "DisplayDomain": "STRING", "Href": "STRING",
                    "DisplayUrlPath": "STRING", "Title2": "STRING", "AdCategories": "STRING", "AdGroupId": "STRING",
                    "CampaignId": "STRING", "Id": "STRING"
                }
            },
            "KEYWORD": {
                "fields": {
                    "Id": "STRING", "Keyword": "STRING", "AdGroupId": "STRING", "CampaignId": "STRING"
                }
            }

        }

        self.tables_with_schema, self.string_fields, self.integer_fields, \
        self.float_fields = create_fields(client_name, "YandexDirect", self.report_dict)

    def __create_body(self, selection_criteria, field_names, limit, offset, **kwargs):
        body = {
            "method": "get",
            "params": {
                "SelectionCriteria": selection_criteria,
                "FieldNames": field_names,
                "Page": {
                    "Limit": limit,
                    "Offset": offset
                }

            }
        }
        body['params'].update(kwargs)
        jsonBody = json.dumps(body, ensure_ascii=False).encode('utf8')
        return jsonBody

    def __request(self, selection_criteria, field_names, method, limit, offset, total_list, key, **kwargs):
        jsonBody = self.__create_body(selection_criteria, field_names, limit, offset, **kwargs)
        try:
            data = requests.post(self.url+method, jsonBody, headers=self.headers_report).json()
        except requests.exceptions.ConnectionError as error:
            print(error)
            data = requests.post(self.url + method, jsonBody, headers=self.headers_report).json()
        total_list += data['result'][key]
        if data['result'].get("LimitedBy", False):
            offset += limit
            return self.__request(selection_criteria, field_names, method, limit, offset, total_list, key)
        return total_list

    def get_agency_clients(self):
        selection_criteria = {"Archived": "NO"}
        field_names = ["ClientId", "ClientInfo", "Login"]
        clients = self.__request(selection_criteria, field_names, "agencyclients", 10000, 0, [], "Clients")
        client_list = [client['Login'] for client in clients]
        return clients, client_list

    def get_campaigns(self):
        self.headers_report['Client-Login'] = self.client_login
        selection_criteria = {}
        field_names = ["Id", "Name", "Type"]
        campaigns = self.__request(selection_criteria, field_names, "campaigns", 10000, 0, [], "Campaigns")
        return campaigns

    def get_adsets(self, campaign_ids_list):
        result_adsets = []
        slice_ids = my_slice(campaign_ids_list, 10)
        self.headers_report['Client-Login'] = self.client_login
        field_names = ["CampaignId", "Id", "Name", "Type"]
        for ids in slice_ids:
            selection_criteria = {"CampaignIds": ids}
            adsets = self.__request(selection_criteria, field_names, "adgroups", 10000, 0, [], "AdGroups")
            result_adsets += adsets
        return result_adsets

    def get_ads(self, campaign_ids_list):
        result_ads = []
        slice_ids = my_slice(campaign_ids_list, 10)
        self.headers_report['Client-Login'] = self.client_login
        for ids in slice_ids:
            selection_criteria = {"CampaignIds": ids}
            field_names = ["AdCategories", "AdGroupId", "CampaignId", "Id", "Type"]
            params = {
                "TextAdFieldNames": ["DisplayDomain", "Href", "Text", "Title", "Title2", "DisplayUrlPath"],
                "TextImageAdFieldNames": ["Href"],
                "TextAdBuilderAdFieldNames": ["Href"],
                "CpcVideoAdBuilderAdFieldNames": ["Href"],
                "CpmBannerAdBuilderAdFieldNames": ["Href"],
                "CpmVideoAdBuilderAdFieldNames": ["Href"]}
            ads = self.__request(selection_criteria, field_names, "ads", 10000, 0, [], "Ads", **params)
            result_ads += [expand_dict(ad, {}, {}) for ad in ads]
        return result_ads

    def get_keywords(self, campaign_ids_list):
        result_keywords = []
        slice_ids = my_slice(campaign_ids_list, 10)
        self.headers_report['Client-Login'] = self.client_login
        field_names = ["Id", "Keyword", "AdGroupId", "CampaignId"]
        for ids in slice_ids:
            selection_criteria = {"CampaignIds": ids}
            keywords = self.__request(selection_criteria, field_names, "keywords", 10000, 0, [], "Keywords")
            result_keywords += keywords
        return result_keywords


