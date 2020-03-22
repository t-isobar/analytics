#!usr/bin/python3

import requests, json, time
import pandas as pd
from datetime import datetime
import re
import _BigQuery, _Utils


class YandexDirect:
    def __init__(self, access_token, client_login, client_name):
        self.url = "https://api.direct.yandex.com/json/v5/"
        self.headers_report = {
           "Authorization": "Bearer " + access_token,
           "Client-Login": client_login,
           "Accept-Language": "ru",
           "processingMode": "auto",
           "returnMoneyInMicros": "false",
           "skipReportHeader": "true",
           "skipReportSummary": "true"}

        self.report_dict = {
            "CAMPAIGN_STAT": {"type": "CUSTOM_REPORT",
            "fields": {"Date": "STRING", "CampaignId": "STRING", "CampaignName": "STRING", "CampaignType": "STRING",
                       "Impressions": "INTEGER", "Cost": "FLOAT", "Clicks": "INTEGER"}},

            "CAMPAIGN_DEVICE_AND_PLACEMENT_STAT": {"type": "CUSTOM_REPORT",
            "fields": {"Date": "STRING", "CampaignId": "STRING", "CampaignType": "STRING", "CampaignName": "STRING",
                       "AdNetworkType": "STRING", "Device": "STRING", "Impressions": "INTEGER", "Cost": "FLOAT",
                       "Clicks": "INTEGER"}},

            "CAMPAIGN_GEO_STAT": {"type": "CAMPAIGN_PERFORMANCE_REPORT",
            "fields": {"Date": "STRING", "CampaignId": "STRING", "CampaignName": "STRING", "CampaignType": "STRING",
                       "LocationOfPresenceName": "STRING", "TargetingLocationId": "STRING", "Impressions": "INTEGER",
                       "Cost": "FLOAT", "Clicks": "INTEGER"}},

            "CAMPAIGN_PLACEMENT_STAT": {"type": "CAMPAIGN_PERFORMANCE_REPORT",
            "fields": {"Date": "STRING", "CampaignId": "STRING", "CampaignName": "STRING",  "CampaignType": "STRING",
                       "Placement": "STRING", "Impressions": "INTEGER", "Cost": "FLOAT", "Clicks": "INTEGER"}},

            "CAMPAIGN_SOCDEM_DEVICE_STAT": {"type": "CAMPAIGN_PERFORMANCE_REPORT",
            "fields": {"Date": "STRING", "CampaignId": "STRING", "CampaignName": "STRING", "Device": "STRING",
                       "CarrierType": "STRING", "CampaignType": "STRING",  "MobilePlatform": "STRING", "Age": "STRING",
                       "Gender": "STRING", "Impressions": "INTEGER", "Cost": "FLOAT", "Clicks": "INTEGER"}},

            "AD_STAT": {"type": "AD_PERFORMANCE_REPORT",
            "fields": {"Date": "STRING", "CampaignId": "STRING", "CampaignName": "STRING", "CampaignType": "STRING",
                       "AdId": "STRING", "AdFormat": "STRING", "AdGroupId": "STRING", "AdGroupName": "STRING",
                       "Impressions": "INTEGER", "Cost": "FLOAT", "Clicks": "INTEGER"}},

            "AD_DEVICE_STAT": {"type": "AD_PERFORMANCE_REPORT",
            "fields": {"Date": "STRING", "CampaignId": "STRING", "CampaignName": "STRING", "CampaignType": "STRING",
                       "AdId": "STRING", "AdFormat": "STRING", "AdGroupId": "STRING", "AdGroupName": "STRING",
                       "Device": "STRING", "AdNetworkType": "STRING", "Impressions": "INTEGER", "Cost": "FLOAT",
                       "Clicks": "INTEGER"}},

            "REACH_AND_FREQUENCY_STAT": {"type": "REACH_AND_FREQUENCY_PERFORMANCE_REPORT",
            "fields": {"Date": "STRING", "CampaignId": "STRING", "CampaignName": "STRING", "CampaignType": "STRING",
                       "AdGroupId": "STRING", "AdGroupName": "STRING", "AdId": "STRING", "Impressions": "INTEGER",
                       "ImpressionReach": "INTEGER", "AvgImpressionFrequency": "FLOAT", "Cost": "FLOAT", "Clicks": "INTEGER"}},

            "KEYWORD_AD_STAT": {"type": "CUSTOM_REPORT",
            "fields": {"Date": "STRING", "CampaignId": "STRING", "CampaignName": "STRING", "CampaignType": "STRING",
                       "AdGroupId": "STRING", "AdGroupName": "STRING", "AdId": "STRING", "Impressions": "INTEGER",
                       "Clicks": "INTEGER", "Cost": "FLOAT", "CriterionId": "STRING", "Criterion": "STRING",
                       "CriteriaType": "STRING"}},

            "KEYWORD_SOCDEM_STAT": {"type": "CUSTOM_REPORT",
            "fields": {"Date": "STRING", "CampaignId": "STRING", "CampaignName": "STRING", "CampaignType": "STRING",
                       "AdGroupId": "STRING", "AdGroupName": "STRING", "AdId": "STRING", "CriterionId": "STRING",
                       "Criterion": "STRING", "CriteriaType": "STRING", "Slot": "STRING", "Age": "STRING", "Gender": "STRING",
                       "Impressions": "INTEGER", "Cost": "FLOAT", "Clicks": "INTEGER"}},

            "KEYWORD_DEVICE_STAT": {"type": "CRITERIA_PERFORMANCE_REPORT",
            "fields": {"Date": "STRING", "CampaignId": "STRING", "CampaignName": "STRING", "CampaignType": "STRING",
                       "Device": "STRING", "Impressions": "INTEGER", "Cost": "FLOAT", "Clicks": "INTEGER",
                       "CriterionId": "STRING", "Criterion": "STRING", "CriteriaType": "STRING"}},

            "KEYWORD_DEVICE_AD_STAT": {"type": "CUSTOM_REPORT",
            "fields": {"Date": "STRING", "CampaignId": "STRING", "CampaignName": "STRING", "CampaignType": "STRING",
                       "AdGroupId": "STRING", "AdGroupName": "STRING", "AdId": "STRING", "Impressions": "INTEGER",
                       "Cost": "FLOAT", "Clicks": "INTEGER", "CriterionId": "STRING", "Criterion": "STRING",
                       "CriterionType": "STRING", "AdNetworkType": "STRING", "Device": "STRING", "Slot": "STRING",
                       "Placement": "STRING", "TargetingLocationId": "STRING", "TargetingLocationName": "STRING"}}
        }

        self.tables_with_schema = {f"{client_name}_YandexDirect_{report_name}": self.report_dict[report_name]['fields'] for report_name in list(self.report_dict.keys())}

        self.string_fields = list(set([key for values in self.report_dict.values() for key, value in values['fields'].items() if value == "STRING"]))
        self.integer_fields = list(set([key for values in self.report_dict.values() for key, value in values['fields'].items() if value == "INTEGER"]))
        self.float_fields = list(set([key for values in self.report_dict.values() for key, value in values['fields'].items() if value == "FLOAT"]))

    def __create_body(self, selection_criteria, field_names, report_name, report_type):
        body = {
            "params": {
                "SelectionCriteria": selection_criteria,
                "FieldNames": field_names,
                "ReportName": (report_name),
                "ReportType": report_type,
                "DateRangeType": "CUSTOM_DATE",
                "Format": "TSV",
                "IncludeVAT": "NO",
                "IncludeDiscount": "NO"
            }
        }
        jsonBody = json.dumps(body, ensure_ascii=False).encode('utf8')
        return jsonBody

    def __request(self, selection_criteria, field_names, report_name, report_type, method):
        jsonBody = self.__create_body(selection_criteria, field_names, report_name, report_type)
        data = requests.post(self.url+method, jsonBody, headers=self.headers_report)
        if data.status_code in [201, 202]:
            time.sleep(60)
            return self.__request(selection_criteria, field_names, report_name, report_type, method)
        return data

    def get_report(self, report_type, report_name, date_from, date_to):
        """
        report_name - report_type - fields:
         - CAMPAIGN_STAT - CUSTOM_REPORT
         - CAMPAIGN_DEVICE_AND_PLACEMENT_STAT - CUSTOM_REPORT
         - CAMPAIGN_GEO_STAT - CAMPAIGN_PERFORMANCE_REPORT
         - CAMPAIGN_PLACEMENT_STAT - CAMPAIGN_PERFORMANCE_REPORT
         - CAMPAIGN_SOCDEM_DEVICE_STAT - CAMPAIGN_PERFORMANCE_REPORT
         - AD_STAT - AD_PERFORMANCE_REPORT
         - AD_DEVICE_STAT - AD_PERFORMANCE_REPORT
         - REACH_AND_FREQUENCY_STAT - REACH_AND_FREQUENCY_PERFORMANCE_REPORT
         - KEYWORD_AD_STAT - CUSTOM_REPORT
         - KEYWORD_SOCDEM_STAT - CUSTOM_REPORT
         - KEYWORD_DEVICE_STAT - CRITERIA_PERFORMANCE_REPORT
         - KEYWORD_DEVICE_AD_STAT - CUSTOM_REPORT

         date format: "YYYY-MM-DD"

        """

        get_data_params = self.report_dict.get(report_type, False)
        if get_data_params:
            selection_criteria = {"DateFrom": date_from, "DateTo": date_to}
            field_names = list(get_data_params['fields'].keys())
            data = self.__request(selection_criteria, field_names, report_name, get_data_params['type'], "reports")
            return data
        else:
            return "Указан недопустимый тип отчета."


def getYandexReport(access_token, client_name, client_login, path_to_bq, date_from, date_to, period):
	client_login_re = re.sub('[.-]', '_', client_login)
	bq = _BigQuery.BigQuery(path_to_bq)
	yandex = YandexDirect(access_token, client_login, client_name)
	ut = _Utils.Utils()

	dataset_ID = f"{client_name}_YandexDirect_{client_login_re}"

	date_range = ut.slice_date_on_period(date_from, date_to, period)

	bq.check_or_create_dataset(dataset_ID)
	bq.check_or_create_tables(yandex.tables_with_schema, dataset_ID)

	for report in yandex.report_dict:
            for date_from, date_to in date_range:
                report_data = yandex.get_report(f"{report}", f"{report} {datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')}", date_from, date_to)
                report_data_split = report_data.text.split('\n')
                data = [x.split('\t') for x in report_data_split]
                stat = pd.DataFrame(data[1:-1], columns=data[:1][0])

                bq.data_to_insert(stat, yandex.integer_fields, yandex.float_fields, yandex.string_fields, dataset_ID, f"{client_name}_YandexDirect_{report}")