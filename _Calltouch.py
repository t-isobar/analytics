#!usr/bin/python3

import requests, sys
import _BigQuery
import pandas as pd

class Calltouch:
    def __init__(self, ct_site_id, ct_token, client_name):
        self.__ct_token = ct_token
        self.__url = f'http://api.calltouch.ru/calls-service/RestAPI/{ct_site_id}/calls-diary/calls'
        self.report_dict = {
    "CALLS": {
        "fields": {'date': "STRING", 'callUrl': "STRING", 'uniqueCall': "STRING", 'utmContent': "STRING", 'source': "STRING", 'waitingConnect': "FLOAT",
        'ctCallerId': "STRING", 'keyword': "STRING", 'utmSource': "STRING", 'sipCallId': "STRING", 'utmCampaign': "STRING", 'phoneNumber': "STRING",
        'uniqTargetCall': "STRING", 'utmMedium': "STRING", 'city': "STRING", 'yaClientId': "STRING", 'medium': "STRING", 'duration': "FLOAT",
        'callbackCall': "STRING", 'successful': "STRING", 'callId': "STRING", 'clientId': "STRING", 'callerNumber': "STRING", 'utmTerm': "STRING",
        'sessionId': "STRING", 'targetCall': "STRING", 'AUTO_PR': "STRING", 'MANUAL': "STRING"}}
}

        self.tables_with_schema = {f"{client_name}_Calltouch_{report_name}": self.report_dict[report_name]['fields'] for report_name in list(self.report_dict.keys())}

        self.string_fields = list(set([key for values in self.report_dict.values() for key, value in values['fields'].items() if value == "STRING"]))
        self.integer_fields = list(set([key for values in self.report_dict.values() for key, value in values['fields'].items() if value == "INTEGER"]))
        self.float_fields = list(set([key for values in self.report_dict.values() for key, value in values['fields'].items() if value == "FLOAT"]))

    def __get_pages(self, dateFrom, dateTo):
        params = {'clientApiId': self.__ct_token, 'dateFrom': dateFrom, 'dateTo': dateTo, 'page': 1, 'limit': 1000}
        response = requests.get(self.__url, params = params).json()['pageTotal']
        return response

    def get_calls(self, dateFrom, dateTo):
        i = 1
        total_result = []
        pages = self.__get_pages(dateFrom, dateTo)
        keys = ['callId', 'callerNumber', 'date', 'waitingConnect', 'duration', 'phoneNumber', 'successful', 'uniqueCall',
                'targetCall', 'uniqTargetCall', 'callbackCall', 'city', 'source', 'medium', 'keyword', 'callUrl', 'utmSource',
                'utmMedium', 'utmCampaign', 'utmContent', 'utmTerm', 'sessionId', 'ctCallerId', 'clientId', 'yaClientId',
                'sipCallId', 'callTags', 'callUrl']

        while i <= pages:
            params = {'clientApiId': self.__ct_token, 'dateFrom': dateFrom, 'dateTo': dateTo, 'page': i, 'limit': 1000,
                      'withCallTags': True}
            list_of_calls = requests.get(self.__url, params = params).json()['records']
            i += 1
            for call in list_of_calls:
                data = call.copy()
                for key, values in call.items():
                    if key not in keys:
                        data.pop(key)
                    elif key == 'callTags':
                        for one in values:
                            if one['type'] == 'AUTO-PR':
                                data["AUTO_PR"] = ",".join(one['names'])
                            elif one['type'] == 'MANUAL':
                                data["MANUAL"] = ",".join(one['names'])
                        data.pop("callTags")
                total_result.append(data)

        return total_result


def getCalltouchReport(access_token, site_id, client_name, path_to_bq, date_from, date_to):
    ct = Calltouch(site_id, access_token, client_name)
    bq = _BigQuery(path_to_bq)

    dataset_ID = f"{client_name}_Calltouch_{site_id}"

    bq.check_or_create_dataset(dataset_ID)
    bq.check_or_create_tables(ct.tables_with_schema, dataset_ID)

    calls = ct.get_calls(date_from, date_to)
    if calls == []:
        sys.exit(f"За {date_from} - {date_to} нет статистики")

    calls_df = pd.DataFrame(calls).fillna("<not set>")
    bq.data_to_insert(calls_df, ct.integer_fields, ct.float_fields, ct.string_fields, dataset_ID, f"{client_name}_Calltouch_CALLS")
