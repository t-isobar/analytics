import json, requests
import re
from analytics.connectors._Utils import create_fields_ga


class YandexMetrica:
    def __init__(self, access_token, client_name, view_id):
        self.access_token = access_token
        self.client_name = client_name
        self.view_id = view_id
        self.url = "https://api-metrika.yandex.net/stat/v1/data"
        self.headers = {
            "Authorization": "OAuth" + self.access_token
        }

        self.report_dict = {
            "BASE": {
                "metrics": {
                    "ym_s_newUsers": "INTEGER", "ym_s_pageDepth": "FLOAT", "ym_s_pageviews": "INTEGER",
                    "ym_s_users": "INTEGER", "ym_s_visits": "INTEGER"},
                "dimensions": {
                    "ym_s_date": "STRING", "ym_s_UTMTerm": "STRING", "ym_s_UTMSource": "STRING",
                    "ym_s_UTMMedium": "STRING", "ym_s_UTMContent": "STRING", "ym_s_UTMCampaign": "STRING"}
            },
            "CONVERSIONS": {
                "metrics": {},
                "dimensions": {
                    "ym_s_date": "STRING", "ym_s_UTMTerm": "STRING", "ym_s_UTMSource": "STRING",
                    "ym_s_UTMMedium": "STRING", "ym_s_UTMContent": "STRING", "ym_s_UTMCampaign": "STRING"}
            }
        }

        self.tables_with_schema, self.string_fields, self.integer_fields, \
        self.float_fields = create_fields_ga(client_name, "YandexMetrica", self.report_dict)

    def get_errors(self, response):
        if response.status_code == 200:
            return response.json()
        else:
            error = json.loads(response.text)
            raise Exception(error['message'])

    def create_conv_schema(self, conversion_ids_list):
        conversion_dict = {}
        for conversion_id in conversion_ids_list:
            conversion_dict[f"ym_s_goal{conversion_id}reaches"] = "INTEGER"
        return conversion_dict

    def get_request(self, dimensions, metrics, date_from, date_to, limit, offset):

        params = {"dimensions": dimensions, "metrics": metrics, "id": self.view_id, "date1": date_from,
                  "date2": date_to, "accuracy": "full", "limit": limit, "offset": offset}
        result = requests.get(self.url, headers=self.headers, params=params)
        response = self.get_errors(result)
        response_data = []
        for element in response['data']:
            response_data.append([value['name'] for value in element['dimensions']] + element['metrics'])
        names = response['query']['dimensions'] + response['query']['metrics']
        return response_data, response['total_rows'], names

    def get_report(self, date_from, date_to, metric_list, dimension_list, limit=100, offset=1, result_data=None):
        if result_data is None:
            result_data = []
        dimensions = ",".join(dimension_list)
        metrics = ",".join(metric_list)
        total_rows = 2

        while offset < total_rows:
            response, total_rows, names = self.get_request(dimensions, metrics, date_from, date_to, limit, offset)
            offset += limit
            result_data += response
        return [names, result_data]



