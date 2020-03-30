import os, json
from datetime import datetime, timedelta
from connectors.mainReport import Report
from connectors import access_data

date_from = datetime.strftime(datetime.today() - timedelta(days=1), "%Y-%m-%d")
date_to = datetime.strftime(datetime.today() - timedelta(days=1), "%Y-%m-%d")
report = Report("TRAFFIC", access_data.path_to_traffic_bq, access_data.path_to_traffic_ga, date_from, date_to)

list_of_logins = report.get_yandex_direct_agency_clients(access_data.yandex_token_general)


clients = json.load(open(os.path.join(os.path.split(os.getcwd())[0], "connectors\json_files", "clients.json"), "r"))
for client, placement in clients.items():
    yandex_params = placement.get('yandex', None)
    if yandex_params:
        project = placement['bigquery']['project']
        for params in yandex_params:
            if project == 'traffic':
                path_to_bq = access_data.path_to_traffic_bq
                path_to_ga = access_data.path_to_traffic_ga
            elif project == 'lenta':
                path_to_bq = access_data.path_to_lenta_bq
                path_to_ga = access_data.path_to_lenta_ga
            else:
                raise Exception("Нет такого проекта")
            print(params['Login'])
            report = Report(client, path_to_bq, path_to_ga, date_from, date_to)
            report.get_yandex_report(params['Login'], access_data.yandex_token_general, 5,
                                     ["CAMPAIGN_STAT", "CAMPAIGN_GEO_STAT", "CAMPAIGN_SOCDEM_DEVICE_STAT"])
