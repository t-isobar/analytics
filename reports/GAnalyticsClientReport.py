import os, json
from datetime import datetime, timedelta
from connectors.mainReport import Report
from connectors import access_data

date_from = datetime.strftime(datetime.today() - timedelta(days=1), "%Y-%m-%d")
date_to = datetime.strftime(datetime.today() - timedelta(days=1), "%Y-%m-%d")

clients = json.load(open(os.path.join(os.path.split(os.getcwd())[0], "connectors\json_files", "clients.json"), "r"))
for client, placement in clients.items():
    analytics_params = placement.get('analytics', None)
    if analytics_params:
        project = placement['bigquery']['project']
        for params in analytics_params:
            if project == 'traffic':
                path_to_bq = access_data.path_to_traffic_bq
                path_to_ga = access_data.path_to_traffic_ga
            elif project == 'lenta':
                path_to_bq = access_data.path_to_lenta_bq
                path_to_ga = access_data.path_to_lenta_ga
            else:
                raise Exception("Нет такого проекта")
            print(params['view_id'])
            report = Report(client, path_to_bq, path_to_ga, date_from, date_to)
            report.get_analytics_report(params['view_id'], ["General", "Goal1to4", "Goal5to8", "Goal9to12",
                                                            "Goal13to16", "Goal17to20"])

