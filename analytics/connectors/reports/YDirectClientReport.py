import os, json
from datetime import datetime, timedelta
from analytics.connectors.mainReport import Report
from analytics.connectors import access_data

project = 't-isobar'
path_to_bq = os.path.join(access_data.path_to_json,
                          access_data.name_json_files['project'][project]['path_to_bq'])

date_from = datetime.strftime(datetime.today() - timedelta(days=1), "%Y-%m-%d")
date_to = datetime.strftime(datetime.today() - timedelta(days=1), "%Y-%m-%d")
report = Report("TRAFFIC", path_to_bq, date_from, date_to)

list_of_logins = report.get_yandex_direct_agency_clients(access_data.yandex_token_general)


clients = json.load(open(os.path.join(os.path.split(os.getcwd())[0], "json_files", "clients.json"), "r"))
for client, placement in clients.items():
    yandex_params = placement.get('yandex', None)
    if yandex_params:
        project = placement['bigquery']['project']
        path_to_bq = os.path.join(access_data.path_to_json,
                                  access_data.name_json_files['project'][project]['path_to_bq'])

        for params in yandex_params:

            report = Report(client, path_to_bq, date_from, date_to)
            report.get_yandex_report(params['Login'], access_data.yandex_token_general, 5,
                                     ["CAMPAIGN_STAT", "CAMPAIGN_GEO_STAT", "CAMPAIGN_SOCDEM_DEVICE_STAT", "AD_STAT"])
