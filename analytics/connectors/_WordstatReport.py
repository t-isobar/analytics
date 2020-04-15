import json, time, requests
from datetime import datetime, timedelta


class WordStat:
    def __init__(self, access_token):
        self.access_token = access_token
        self.__url = 'https://api.direct.yandex.ru/v4/json/'

    def get_response(self, method, params=None):
        if params is None:
            params = {}

        data = {'method': method, 'token': self.access_token, "param": params}

        jdata = json.dumps(data, ensure_ascii=False).encode('utf8')
        response = requests.post(self.__url, jdata)
        response = self.error_handler(response)
        return response

    def error_handler(self, response):
        if response.status_code == 200:
            response = response.json()
            return response
        else:
            raise Exception(json.loads(response.text))

    def create_new_wordstat_report(self, phrases, geo):
        response = self.get_response('CreateNewWordstatReport', {'Phrases': phrases, 'GeoID': geo})
        report_id = response['data']
        response = self.get_wordstat_report_by_id(report_id)
        self.delete_by_id_wordstat_report(report_id)
        return response

    def get_phrase_frequency(self, phrases, geo):
        phrase_frequency = self.create_new_wordstat_report(phrases, geo)
        stat = []
        for phrase in phrase_frequency['data']:
            phrase = phrase['SearchedWith'][0]
            phrase['period'] = f"{datetime.strftime(datetime.today() - timedelta(days=7), '%d.%m.%Y')} " \
                f"- {datetime.strftime(datetime.today(), '%d.%m.%Y')}"
            stat.append(phrase)
        return stat

    def get_regions(self):
        regions = self.get_response('GetRegions')
        return regions

    def get_wordstat_report_by_id(self, report_id):
        report_list = self.get_all_wordstat_report_list()

        for element in report_list:
            if element['ReportID'] == report_id:
                if element['StatusReport'] == 'Done':
                    data = self.get_wordstat_report(report_id)
                    return data
                else:
                    time.sleep(7)
                    return self.get_wordstat_report_by_id(report_id)

    def get_all_wordstat_report_list(self):
        report_list = self.get_response('GetWordstatReportList')

        return report_list['data']

    def delete_all_wordstat_report_list(self):
        all_reports = self.get_all_wordstat_report_list()
        list_of_reports_id = []

        for element in all_reports:
            list_of_reports_id.append(element['ReportID'])
        for report_id in list_of_reports_id:

            result = self.get_response('DeleteWordstatReport', report_id)
            if result['data'] == 1:
                continue

    def delete_by_id_wordstat_report(self, report_id):
        data = self.get_response('DeleteWordstatReport', report_id)

        if data['data'] == 1:
            return []
        else:
            raise Exception('Отчет удалить не удалось.')

    def get_wordstat_report(self, report_id):
        data = self.get_response('GetWordstatReport', report_id)

        return data

    def get_searched_with_words(self, report_id):
        data = self.get_response('GetWordstatReport', report_id)

        return data['data'][0]['SearchedWith']
