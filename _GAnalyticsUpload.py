from apiclient.http import MediaFileUpload
from google.cloud import bigquery
import time, re, sys, os
from apiclient.discovery import build
from google.oauth2 import service_account
import socket
from datetime import datetime, timedelta
import pandas as pd
import _BigQuery


class GAnalyticsUpload:
    def __init__(self, path_to_json, account_id, web_property_id, custom_data_source_id):
        self.KEY_FILE_LOCATION = path_to_json
        self.SCOPES = ["https://www.googleapis.com/auth/analytics"]
        self.account_id = account_id
        self.web_property_id = web_property_id
        self.custom_data_source_id = custom_data_source_id
        self.credentials = service_account.Credentials.from_service_account_file(path_to_json)
        self.scoped_credentials = self.credentials.with_scopes(self.SCOPES)
        self.analytics = build('analytics', 'v3', credentials=self.scoped_credentials)
    
    def upload_data(self, data_frame, path_to_csv, file_name):
        """
        ga:date - дата
        ga:medium - канал
        ga:source - источник
        ga:adClicks - клики
        ga:adCost - стоимость
        ga:impressions - показы
        ga:adContent - содержание объявления
        ga:campaign - кампания
        ga:keyword - ключевое слово
        
        """
        data_frame.to_csv(path_to_csv+file_name, decimal=".",index=False)
        media = MediaFileUpload(path_to_csv+file_name, mimetype='application/octet-stream', resumable=False)
        daily_upload = self.analytics.management().uploads().uploadData(accountId=self.account_id,
                                                                   webPropertyId=self.web_property_id,
                                                                   customDataSourceId=self.custom_data_source_id,
                                                                   media_body=media).execute()
        daily_upload_id = daily_upload['id']
        status, message = self.check_upload_status(daily_upload_id)
        return status, message
        
    def check_upload_status(self, daily_upload_id):
        uploads = self.analytics.management().uploads().list(accountId=self.account_id, webPropertyId=self.web_property_id,
                                                        customDataSourceId=self.custom_data_source_id).execute()
        for upload in uploads['items']:
            if upload['id'] == daily_upload_id:
                status = upload['status']
                if status == 'FAILED':
                    return status, upload['errors']
                elif status in ['PENDING', 'DELETING']:
                    time.sleep(5)
                    return self.check_upload_status(daily_upload_id)
                else:
                    return status, "success"

def UploadDataToGAnalytics(sql_query, rename_schema, source, medium, path_to_ga, path_to_bq, account_id, web_property_id, custom_data_source_id, path_to_csv, file_name, from_date_format):
    bq = _BigQuery.BigQuery(path_to_bq)
    data_frame_to_insert = bq.get_select_query(sql_query)
    
    data_frame_to_insert = data_frame_to_insert.rename(columns=rename_schema)
    data_frame_to_insert['ga:medium'] = medium
    data_frame_to_insert['ga:source'] = source
    data_frame_to_insert['ga:date'] = data_frame_to_insert['ga:date'].apply(lambda x: datetime.strftime(datetime.strptime(x, from_date_format), "%Y%m%d"))

    ga_upload = GAnalyticsUpload(path_to_ga, account_id, web_property_id, custom_data_source_id)
    status, message = ga_upload.upload_data(data_frame_to_insert, path_to_csv, file_name)
    return status, message
