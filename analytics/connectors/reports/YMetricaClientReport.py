import os, json, time
from datetime import datetime, timedelta
from analytics.connectors.mainReport import Report
from analytics.connectors import access_data

project = 't-isobar'
conversions_list = [96762097, 96762190, 96762199, 96762244, 96762481, 96762514, 96762970, 96762976, 96762988,
                    96762994, 96763231, 96763237]
path_to_bq = os.path.join(access_data.path_to_json,
                          access_data.name_json_files['project'][project]['path_to_bq'])

date_from = "2020-03-01"
date_to = "2020-04-12"
# date_from = datetime.strftime(datetime.today() - timedelta(days=1), "%Y-%m-%d")
# date_to = datetime.strftime(datetime.today() - timedelta(days=1), "%Y-%m-%d")


clients = json.load(open(os.path.join(os.path.split(os.getcwd())[0], "json_files", "clients.json"), "r"))
for client, placement in clients.items():
    yandex_params = placement.get('metrica', None)
    if yandex_params:
        project = placement['bigquery']['project']
        path_to_bq = os.path.join(access_data.path_to_json,
                                  access_data.name_json_files['project'][project]['path_to_bq'])

        report = Report(client, path_to_bq, date_from, date_to)
        report.get_metrica_report(access_data.yandex_token_general, yandex_params, ["BASE", "CONVERSIONS"],
                                  conversions_list)




