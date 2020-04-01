import os, json, re
from datetime import datetime, timedelta
from connectors.mainReport import Report
from connectors import access_data

date_from = datetime.strftime(datetime.today() - timedelta(days=1), "%d/%m/%Y")
date_to = datetime.strftime(datetime.today() - timedelta(days=1), "%d/%m/%Y")

clients = json.load(open(os.path.join(os.path.split(os.getcwd())[0], "connectors\json_files", "clients.json"), "r"))
for client, placement in clients.items():
    calltouch_params = placement.get('calltouch', None)
    if calltouch_params:
        project = placement['bigquery']['project']
        for params in calltouch_params:
            path_to_bq = access_data.path_to_traffic_bq
            path_to_ga = access_data.path_to_traffic_ga
            # if project == 'traffic':
            #     path_to_bq = access_data.path_to_traffic_bq
            #     path_to_ga = access_data.path_to_traffic_ga
            # elif project == 'lenta':
            #     path_to_bq = access_data.path_to_lenta_bq
            #     path_to_ga = access_data.path_to_lenta_ga
            # else:
            #     raise Exception("Нет такого проекта")
            site_name_re = re.sub('[.-]', '_', params['name'])
            token = access_data.Calltouch[params['site_id']]
            report = Report(client, path_to_bq, path_to_ga, date_from, date_to)
            report.get_calltouch_report(params['site_id'], token, site_name_re)
