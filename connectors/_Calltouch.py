import requests
from connectors._Utils import Utils


class Calltouch:
    def __init__(self, ct_site_id, ct_token, client_name):
        self.__ct_token = ct_token
        self.__url = f'http://api.calltouch.ru/calls-service/RestAPI/{ct_site_id}/calls-diary/calls'
        self.ut = Utils()
        self.report_dict = {
            "CALLS": {
                "fields": {'date': "STRING", 'callUrl': "STRING", 'uniqueCall': "STRING", 'utmContent': "STRING",
                           'source': "STRING", 'waitingConnect': "FLOAT", 'ctCallerId': "STRING", 'keyword': "STRING",
                           'utmSource': "STRING", 'sipCallId': "STRING", 'utmCampaign': "STRING",
                           'phoneNumber': "STRING", 'uniqTargetCall': "STRING", 'utmMedium': "STRING", 'city': "STRING",
                           'yaClientId': "STRING", 'medium': "STRING", 'duration': "FLOAT", 'callbackCall': "STRING",
                           'successful': "STRING", 'callId': "STRING", 'clientId': "STRING", 'callerNumber': "STRING",
                           'utmTerm': "STRING", 'sessionId': "STRING", 'targetCall': "STRING", 'AUTO_PR': "STRING",
                           'MANUAL': "STRING"}}}

        self.tables_with_schema, self.string_fields, self.integer_fields, \
        self.float_fields = self.ut.create_fields(client_name, "Calltouch", self.report_dict)

    def __get_pages(self, date_from, date_to):
        params = {'clientApiId': self.__ct_token, 'dateFrom': date_from, 'dateTo': date_to, 'page': 1, 'limit': 1000}
        response = requests.get(self.__url, params=params).json()['pageTotal']
        return response

    def get_calls(self, date_from, date_to):
        i = 1
        total_result = []
        pages = self.__get_pages(date_from, date_to)
        keys = ['callId', 'callerNumber', 'date', 'waitingConnect', 'duration', 'phoneNumber', 'successful',
                'uniqueCall', 'targetCall', 'uniqTargetCall', 'callbackCall', 'city', 'source', 'medium', 'keyword',
                'callUrl', 'utmSource', 'utmMedium', 'utmCampaign', 'utmContent', 'utmTerm', 'sessionId', 'ctCallerId',
                'clientId', 'yaClientId', 'sipCallId', 'callTags', 'callUrl']

        while i <= pages:
            params = {'clientApiId': self.__ct_token, 'dateFrom': date_from, 'dateTo': date_to, 'page': i,
                      'limit': 1000, 'withCallTags': True}
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

