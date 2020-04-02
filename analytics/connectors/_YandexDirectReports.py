from analytics.connectors._Utils import create_fields
import requests, json, time


class YandexDirectReports:
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
            "CAMPAIGN_STAT": {
                "type":
                    "CUSTOM_REPORT",
                "fields": {
                    "Date": "STRING", "CampaignId": "STRING", "CampaignName": "STRING", "CampaignType": "STRING",
                    "Impressions": "INTEGER", "Cost": "FLOAT", "Clicks": "INTEGER"}},
            "CAMPAIGN_DEVICE_AND_PLACEMENT_STAT": {
                "type":
                    "CUSTOM_REPORT",
                "fields": {
                    "Date": "STRING", "CampaignId": "STRING", "CampaignType": "STRING", "CampaignName": "STRING",
                    "AdNetworkType": "STRING", "Device": "STRING", "Impressions": "INTEGER", "Cost": "FLOAT",
                    "Clicks": "INTEGER"}},
            "CAMPAIGN_GEO_STAT": {
                "type":
                    "CAMPAIGN_PERFORMANCE_REPORT",
                "fields": {
                    "Date": "STRING", "CampaignId": "STRING", "CampaignName": "STRING", "CampaignType": "STRING",
                    "LocationOfPresenceName": "STRING", "TargetingLocationId": "STRING", "Impressions": "INTEGER",
                    "Cost": "FLOAT", "Clicks": "INTEGER"}},
            "CAMPAIGN_PLACEMENT_STAT": {
                "type":
                    "CAMPAIGN_PERFORMANCE_REPORT",
                "fields": {
                    "Date": "STRING", "CampaignId": "STRING", "CampaignName": "STRING",  "CampaignType": "STRING",
                    "Placement": "STRING", "Impressions": "INTEGER", "Cost": "FLOAT", "Clicks": "INTEGER"}},
            "CAMPAIGN_SOCDEM_DEVICE_STAT": {
                "type":
                    "CAMPAIGN_PERFORMANCE_REPORT",
                "fields": {
                    "Date": "STRING", "CampaignId": "STRING", "CampaignName": "STRING", "Device": "STRING",
                    "CarrierType": "STRING", "CampaignType": "STRING",  "MobilePlatform": "STRING", "Age": "STRING",
                    "Gender": "STRING", "Impressions": "INTEGER", "Cost": "FLOAT", "Clicks": "INTEGER"}},
            "AD_STAT": {
                "type":
                    "AD_PERFORMANCE_REPORT",
                "fields": {
                    "Date": "STRING", "CampaignId": "STRING", "CampaignName": "STRING", "CampaignType": "STRING",
                    "AdId": "STRING", "AdFormat": "STRING", "AdGroupId": "STRING", "AdGroupName": "STRING",
                    "Impressions": "INTEGER", "Cost": "FLOAT", "Clicks": "INTEGER"}},
            "AD_DEVICE_STAT": {
                "type":
                    "AD_PERFORMANCE_REPORT",
                "fields": {
                    "Date": "STRING", "CampaignId": "STRING", "CampaignName": "STRING", "CampaignType": "STRING",
                    "AdId": "STRING", "AdFormat": "STRING", "AdGroupId": "STRING", "AdGroupName": "STRING",
                    "Device": "STRING", "AdNetworkType": "STRING", "Impressions": "INTEGER", "Cost": "FLOAT",
                    "Clicks": "INTEGER"}},
            "REACH_AND_FREQUENCY_STAT": {
                "type":
                    "REACH_AND_FREQUENCY_PERFORMANCE_REPORT",
                "fields": {
                    "Date": "STRING", "CampaignId": "STRING", "CampaignName": "STRING", "CampaignType": "STRING",
                       "AdGroupId": "STRING", "AdGroupName": "STRING", "AdId": "STRING", "Impressions": "INTEGER",
                       "ImpressionReach": "INTEGER", "AvgImpressionFrequency": "FLOAT", "Cost": "FLOAT",
                       "Clicks": "INTEGER"}},
            "KEYWORD_AD_STAT": {
                "type":
                    "CUSTOM_REPORT",
                "fields": {
                    "Date": "STRING", "CampaignId": "STRING", "CampaignName": "STRING", "CampaignType": "STRING",
                    "AdGroupId": "STRING", "AdGroupName": "STRING", "AdId": "STRING", "Impressions": "INTEGER",
                    "Clicks": "INTEGER", "Cost": "FLOAT", "CriterionId": "STRING", "Criterion": "STRING",
                    "CriteriaType": "STRING"}},
            "KEYWORD_SOCDEM_STAT": {
                "type":
                    "CUSTOM_REPORT",
                "fields": {
                    "Date": "STRING", "CampaignId": "STRING", "CampaignName": "STRING", "CampaignType": "STRING",
                    "AdGroupId": "STRING", "AdGroupName": "STRING", "AdId": "STRING", "CriterionId": "STRING",
                    "Criterion": "STRING", "CriteriaType": "STRING", "Slot": "STRING", "Age": "STRING",
                    "Gender": "STRING", "Impressions": "INTEGER", "Cost": "FLOAT", "Clicks": "INTEGER"}},
            "KEYWORD_DEVICE_STAT": {
                "type":
                    "CRITERIA_PERFORMANCE_REPORT",
                "fields": {
                    "Date": "STRING", "CampaignId": "STRING", "CampaignName": "STRING", "CampaignType": "STRING",
                    "Device": "STRING", "Impressions": "INTEGER", "Cost": "FLOAT", "Clicks": "INTEGER",
                    "CriterionId": "STRING", "Criterion": "STRING", "CriteriaType": "STRING"}},
            "KEYWORD_DEVICE_AD_STAT": {
                "type":
                    "CUSTOM_REPORT",
                "fields": {
                    "Date": "STRING", "CampaignId": "STRING", "CampaignName": "STRING", "CampaignType": "STRING",
                    "AdGroupId": "STRING", "AdGroupName": "STRING", "AdId": "STRING", "Impressions": "INTEGER",
                    "Cost": "FLOAT", "Clicks": "INTEGER", "CriterionId": "STRING", "Criterion": "STRING",
                    "CriterionType": "STRING", "AdNetworkType": "STRING", "Device": "STRING", "Slot": "STRING",
                    "Placement": "STRING", "TargetingLocationId": "STRING", "TargetingLocationName": "STRING"}}
        }

        self.tables_with_schema, self.string_fields, self.integer_fields, self.float_fields = \
            create_fields(client_name, "YandexDirect", self.report_dict)

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
