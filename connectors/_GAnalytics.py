#!usr/bin/python3


import time, re
from apiclient.discovery import build
from google.oauth2 import service_account
import socket

class GAnalytics:
    def __init__(self, path_to_json, view_id, client_name):
        self.KEY_FILE_LOCATION = path_to_json
        self.SCOPES = ["https://www.googleapis.com/auth/analytics.readonly"]
        self.VIEW_ID = view_id
        self.credentials = service_account.Credentials.from_service_account_file(self.KEY_FILE_LOCATION)
        self.scoped_credentials = self.credentials.with_scopes(self.SCOPES)
        self.analytics = build('analyticsreporting', 'v4', credentials=self.scoped_credentials)

        self.report_dict = {
            "General": {
                "metrics": {
                    "ga_users": "INTEGER", "ga_newUsers": "INTEGER", "ga_sessions": "INTEGER",
                    "ga_bounces": "INTEGER", "ga_sessionDuration": "FLOAT", "ga_pageviews": "INTEGER",
                    "ga_hits": "INTEGER"},
                "dimensions": {
                    "ga_campaign": "STRING", "ga_sourceMedium": "STRING", "ga_keyword": "STRING",
                    "ga_adContent": "STRING", "ga_date": "STRING", "ga_deviceCategory":"STRING"}},
            "Goal1to5ByClientID": {
                "metrics": {
                    "ga_pageviews": "INTEGER", "ga_sessionDuration": "FLOAT", "ga_hits": "INTEGER",
                    "ga_bounces": "INTEGER", "ga_goal1Completions": "INTEGER", "ga_goal2Completions": "INTEGER",
                    "ga_goal3Completions": "INTEGER", "ga_goal4Completions": "INTEGER",
                    "ga_goal5Completions": "INTEGER"},
                "dimensions": {
                    "ga_campaign": "STRING", "ga_sourceMedium": "STRING", "ga_keyword": "STRING",
                    "ga_adContent": "STRING", "ga_date": "STRING", "ga_deviceCategory": "STRING",
                    "ga_userType": "STRING", "ga_city": "STRING", "ga_ClientId": "STRING"}},
            "Goal6to10ByClientID": {
                "metrics": {
                    "ga_pageviews": "INTEGER", "ga_sessionDuration": "FLOAT", "ga_hits": "INTEGER",
                    "ga_bounces": "INTEGER", "ga_goal6Completions": "INTEGER", "ga_goal7Completions": "INTEGER",
                    "ga_goal8Completions": "INTEGER", "ga_goal9Completions": "INTEGER",
                    "ga_goal10Completions": "INTEGER"},
                "dimensions": {
                    "ga_campaign": "STRING", "ga_sourceMedium": "STRING", "ga_keyword": "STRING",
                    "ga_adContent": "STRING", "ga_date": "STRING", "ga_deviceCategory": "STRING",
                    "ga_userType": "STRING", "ga_city": "STRING", "ga_ClientId": "STRING"}},
            "Goal11to15ByClientID": {
                "metrics": {
                    "ga_pageviews": "INTEGER", "ga_sessionDuration": "FLOAT", "ga_hits": "INTEGER",
                    "ga_bounces": "INTEGER", "ga_goal11Completions": "INTEGER", "ga_goal12Completions": "INTEGER",
                    "ga_goal13Completions": "INTEGER", "ga_goal14Completions": "INTEGER",
                    "ga_goal15Completions": "INTEGER"},
                "dimensions": {
                    "ga_campaign": "STRING", "ga_sourceMedium": "STRING", "ga_keyword": "STRING",
                    "ga_adContent": "STRING", "ga_date": "STRING", "ga_deviceCategory": "STRING",
                    "ga_userType": "STRING", "ga_city": "STRING", "ga_ClientId": "STRING"}},
            "Goal16to20ByClientID": {
                "metrics": {
                    "ga_pageviews": "INTEGER", "ga_sessionDuration": "FLOAT", "ga_hits": "INTEGER",
                    "ga_bounces": "INTEGER", "ga_goal16Completions": "INTEGER", "ga_goal17Completions": "INTEGER",
                    "ga_goal18Completions": "INTEGER", "ga_goal19Completions": "INTEGER",
                    "ga_goal20Completions": "INTEGER"},
                "dimensions": {
                    "ga_campaign": "STRING", "ga_sourceMedium": "STRING", "ga_keyword": "STRING",
                    "ga_adContent": "STRING", "ga_date": "STRING", "ga_deviceCategory": "STRING",
                    "ga_userType": "STRING", "ga_city": "STRING", "ga_ClientId": "STRING"}},
            "Goal1to4": {
                "metrics": {
                    "ga_users": "INTEGER", "ga_newUsers": "INTEGER", "ga_sessions": "INTEGER", "ga_bounces": "INTEGER",
                    "ga_sessionDuration": "FLOAT", "ga_pageviews": "INTEGER", "ga_goal1Completions": "INTEGER",
                    "ga_goal2Completions": "INTEGER", "ga_goal3Completions": "INTEGER",
                    "ga_goal4Completions": "INTEGER"},
                "dimensions": {
                    "ga_campaign": "STRING", "ga_sourceMedium": "STRING", "ga_keyword": "STRING",
                    "ga_adContent": "STRING", "ga_date": "STRING", "ga_deviceCategory": "STRING"}},
            "Goal5to8": {
                "metrics": {
                    "ga_users": "INTEGER", "ga_newUsers": "INTEGER", "ga_sessions": "INTEGER", "ga_bounces": "INTEGER",
                    "ga_sessionDuration": "FLOAT", "ga_pageviews": "INTEGER", "ga_goal5Completions": "INTEGER",
                    "ga_goal6Completions": "INTEGER", "ga_goal7Completions": "INTEGER",
                    "ga_goal8Completions": "INTEGER"},
                "dimensions": {
                    "ga_campaign": "STRING", "ga_sourceMedium": "STRING", "ga_keyword": "STRING",
                    "ga_adContent": "STRING", "ga_date": "STRING", "ga_deviceCategory": "STRING"}},
            "Goal9to12": {
                "metrics": {
                    "ga_users": "INTEGER", "ga_newUsers": "INTEGER", "ga_sessions": "INTEGER", "ga_bounces": "INTEGER",
                    "ga_sessionDuration": "FLOAT", "ga_pageviews": "INTEGER", "ga_goal9Completions": "INTEGER",
                    "ga_goal10Completions": "INTEGER", "ga_goal11Completions": "INTEGER",
                    "ga_goal12Completions": "INTEGER"},
                "dimensions": {
                    "ga_campaign": "STRING", "ga_sourceMedium": "STRING", "ga_keyword": "STRING",
                    "ga_adContent": "STRING", "ga_date": "STRING", "ga_deviceCategory": "STRING"}},
            "Goal13to16": {
                "metrics": {
                    "ga_users": "INTEGER", "ga_newUsers": "INTEGER", "ga_sessions": "INTEGER", "ga_bounces": "INTEGER",
                    "ga_sessionDuration": "FLOAT", "ga_pageviews": "INTEGER", "ga_goal13Completions": "INTEGER",
                    "ga_goal14Completions": "INTEGER", "ga_goal15Completions": "INTEGER",
                    "ga_goal16Completions": "INTEGER"},
                "dimensions": {
                    "ga_campaign": "STRING", "ga_sourceMedium": "STRING", "ga_keyword": "STRING",
                    "ga_adContent": "STRING", "ga_date": "STRING", "ga_deviceCategory": "STRING"}},
            "Goal17to20": {
                "metrics": {
                    "ga_users": "INTEGER", "ga_newUsers": "INTEGER", "ga_sessions": "INTEGER", "ga_bounces": "INTEGER",
                    "ga_sessionDuration": "FLOAT", "ga_pageviews": "INTEGER", "ga_goal17Completions": "INTEGER",
                    "ga_goal18Completions": "INTEGER", "ga_goal19Completions": "INTEGER",
                    "ga_goal20Completions": "INTEGER"},
                "dimensions": {
                    "ga_campaign": "STRING", "ga_sourceMedium": "STRING", "ga_keyword": "STRING",
                    "ga_adContent": "STRING", "ga_date": "STRING", "ga_deviceCategory": "STRING"}}}

        self.tables_with_schema, self.string_fields, self.integer_fields, self.float_fields = \
            self.ut.create_fields_ga(client_name, "GAnalytics", self.report_dict)

    def convert_data(self, dimension_list, metric_list, response_data_list):
        columns = dimension_list + metric_list
        total_data_list = []
        for element in response_data_list:
            for one_dict in element:
                total_data_list.append(one_dict['dimensions']+one_dict['metrics'][0]['values'])
        return columns, total_data_list

    def request(self, body):
        try:
            response = self.analytics.reports().batchGet(body=body).execute()
        except socket.timeout:
            time.sleep(2)
            return self.request(body)
        except ConnectionResetError:
            time.sleep(2)
            self.analytics = build('analyticsreporting', 'v4', credentials=self.scoped_credentials)
            return self.request(body)
        return response

    def create_params(self, list_of_params, type_of_metric):
        params_dict = []
        if type_of_metric == "metrics":
            key = 'expression'
        elif type_of_metric == "dimensions":
            key = 'name'
        else:
            raise Exception("Not supported type")
        for param in list_of_params:
            params_dict.append({key: param})
        return params_dict

    def create_body(self, date_from, date_to, metric, dimension, page_token=''):
        body = {
            "reportRequests":
                [{
                    "viewId": self.VIEW_ID,
                    "dateRanges": [{"startDate": date_from, "endDate": date_to}],
                    "metrics": metric,
                    "dimensions": dimension,
                    "samplingLevel": "LARGE",
                    "pageSize": 50000,
                    "pageToken": page_token
                }]
        }
        return body

    def get_request(self, date_from, date_to, metric_list, dimension_list):
        metric = self.create_params(metric_list, 'metrics')
        dimension = self.create_params(dimension_list, 'dimensions')
        response_data_list = []

        body = self.create_body(date_from, date_to, metric, dimension)

        response = self.request(body)
        response_data_list.append(response['reports'][0]['data']['rows'])

        while response['reports'][0].get('nextPageToken') != None:
            page_token = response['reports'][0]['nextPageToken']
            body = self.create_body(date_from, date_to, metric, dimension, page_token=page_token)
            response = self.request(body)
            response_data_list.append(response['reports'][0]['data']['rows'])
            time.sleep(2)
        columns, result_list_of_data = self.convert_data(dimension_list, metric_list, response_data_list)
        return columns, result_list_of_data
