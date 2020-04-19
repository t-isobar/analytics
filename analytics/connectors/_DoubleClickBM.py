from apiclient.discovery import build
from google.oauth2 import service_account
import requests
import pandas as pd
import io
from datetime import datetime

path_to_json = ""
SCOPES = ["https://www.googleapis.com/auth/doubleclickbidmanager"]
credentials = service_account.Credentials.from_service_account_file(path_to_json)
scoped_credentials = credentials.with_scopes(SCOPES)
analytics = build('doubleclickbidmanager', 'v1.1', credentials=scoped_credentials)

# Список всех LineItems
line_items = analytics.lineitems().downloadlineitems().execute()
line_items_df = pd.read_csv(io.StringIO(line_items['lineItems']))

# Запрос на создание Query
# Список всех доступных параметров запроса https://developers.google.com/bid-manager/v1.1/queries?hl=ru
title = f"{datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')}"
body = {
 "kind": "doubleclickbidmanager#query",
 "metadata": {
  "title": title,
  "dataRange": "LAST_30_DAYS",
  "format": "CSV",
  "running": True,
  "sendNotification": False
 },
 "params": {
  "type": "TYPE_GENERAL",
  "groupBys": [
   "FILTER_ADVERTISER_CURRENCY",
   "FILTER_LINE_ITEM",
   "FILTER_DATE",
   "FILTER_INSERTION_ORDER",
   "FILTER_LINE_ITEM_NAME",
   "FILTER_INSERTION_ORDER_NAME"
  ],
  "metrics": [
   "METRIC_IMPRESSIONS",
   "METRIC_CLICKS",
   "METRIC_TOTAL_MEDIA_COST_ADVERTISER"
  ]
 },
#  "schedule": {
#   "frequency": "DAILY"
#  }
}
querie_data = analytics.queries().createquery(body=body).execute()
queryId = querie_data['queryId']

# Получить список запросов
queries = analytics.queries().listqueries(pageSize=25).execute()

queries_dict = {}
for query in queries['queries']:
    queries_dict[query['queryId']] = query['metadata']
    queries_dict[query['queryId']]["group_by_len"] = len(query['params']['groupBys'])


report = requests.get(queries_dict[queryId]['googleCloudStoragePathForLatestReport'])
report_df = pd.read_csv(io.StringIO(report.content.decode("utf8")), skipfooter=7+queries_dict[queryId]['group_by_len'])
