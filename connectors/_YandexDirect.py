import json, requests
from connectors._Utils import expand_dict


class YandexDirect:
    def __init__(self, access_token, client_login):
        self.url = "https://api.direct.yandex.com/json/v5/"
        self.client_login = client_login
        self.headers_report = {
            "Authorization": "Bearer " + access_token,
            "Accept-Language": "ru"}

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
        return clients

    def get_campaigns(self, ):
        self.headers_report['Client-Login'] = self.client_login
        selection_criteria = {}
        field_names = ["Id", "Name", "Type"]
        campaigns = self.__request(selection_criteria, field_names, "campaigns", 10000, 0, [], "Campaigns")
        return campaigns

    def get_adsets(self, campaign_ids_list):
        self.headers_report['Client-Login'] = self.client_login
        selection_criteria = {"CampaignIds": campaign_ids_list}
        field_names = ["CampaignId", "Id", "Name", "Type"]
        adsets = self.__request(selection_criteria, field_names, "adgroups", 10000, 0, [], "AdGroups")
        return adsets

    def get_ads(self, camapign_ids_list):
        self.headers_report['Client-Login'] = self.client_login
        selection_criteria = {"CampaignIds": camapign_ids_list}
        field_names = ["AdCategories", "AdGroupId", "CampaignId", "Id", "Type"]
        params = {
            "TextAdFieldNames": ["DisplayDomain", "Href", "SitelinkSetId", "Text", "Title", "Title2", "DisplayUrlPath",
                                 "AdExtensions"],
            "MobileAppAdFieldNames": ["Title", "Text", "Features", "Action", "TrackingUrl"],
            "TextImageAdFieldNames": ["Href"],
            "MobileAppImageAdFieldNames": ["TrackingUrl"],
            "TextAdBuilderAdFieldNames": ["Href"],
            "MobileAppAdBuilderAdFieldNames": ["TrackingUrl"],
            "CpcVideoAdBuilderAdFieldNames": ["Href"],
            "CpmBannerAdBuilderAdFieldNames": ["Href"],
            "CpmVideoAdBuilderAdFieldNames": ["Href"]}
        ads = self.__request(selection_criteria, field_names, "ads", 10000, 0, [], "Ads", **params)
        result_ads = [expand_dict(ad, {}, {}) for ad in ads]
        return result_ads

    def get_keywords(self, camapign_ids_list):
        self.headers_report['Client-Login'] = self.client_login
        selection_criteria = {"CampaignIds": camapign_ids_list}
        field_names = ["Id", "Keyword", "AdGroupId", "CampaignId"]
        keywords = self.__request(selection_criteria, field_names, "keywords", 10000, 0, [], "Keywords")
        return keywords
