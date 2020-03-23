from apiclient.http import MediaFileUpload
import time
from apiclient.discovery import build
from google.oauth2 import service_account


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
        data_frame.to_csv(path_to_csv+file_name, decimal=".", index=False)
        media = MediaFileUpload(path_to_csv+file_name, mimetype='application/octet-stream', resumable=False)
        daily_upload = self.analytics.management().uploads().uploadData(accountId=self.account_id,
                                                                        webPropertyId=self.web_property_id,
                                                                        customDataSourceId=self.custom_data_source_id,
                                                                        media_body=media).execute()
        daily_upload_id = daily_upload['id']
        status, message = self.check_upload_status(daily_upload_id)
        return status, message
        
    def check_upload_status(self, daily_upload_id):
        uploads = self.analytics.management().uploads().list(accountId=self.account_id,
                                                             webPropertyId=self.web_property_id,
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

