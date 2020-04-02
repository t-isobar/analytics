import os, json
from datetime import datetime, timedelta
from analytics.connectors.mainReport import Report
from analytics.connectors import access_data

date_from = datetime.strftime(datetime.today() - timedelta(days=1), "%Y-%m-%d")
date_to = datetime.strftime(datetime.today() - timedelta(days=1), "%Y-%m-%d")

clients = json.load(open(os.path.join(os.path.split(os.getcwd())[0], "json_files", "clients.json"), "r"))

for client, placement in clients.items():
    yandex_params = placement.get('yandex', None)
    if yandex_params:
        project = placement['bigquery']['project']
        path_to_bq = os.path.join(access_data.path_to_json,
                                  access_data.name_json_files['project'][project]['path_to_bq'])

        for params in yandex_params:
            yandex = Report(client, path_to_bq, date_from, date_to)
            yandex.get_yandex_direct_objects(access_data.yandex_token_general, "suprastinex2020")
