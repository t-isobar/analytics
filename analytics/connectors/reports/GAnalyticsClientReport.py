import os, json
from datetime import datetime, timedelta
from analytics.connectors.mainReport import Report
from analytics.connectors import access_data

date_from = datetime.strftime(datetime.today() - timedelta(days=1), "%Y-%m-%d")
date_to = datetime.strftime(datetime.today() - timedelta(days=1), "%Y-%m-%d")

clients = json.load(open(os.path.join(os.path.split(os.getcwd())[0], "json_files", "clients.json"), "r"))
for client, placement in clients.items():
    analytics_params = placement.get('analytics', None)
    if analytics_params:
        project = placement['bigquery']['project']
        path_to_bq = os.path.join(access_data.path_to_json,
                                  access_data.name_json_files['project'][project]['path_to_bq'])

        path_to_ga = os.path.join(access_data.path_to_json,
                                  access_data.name_json_files['project'][project]['path_to_ga'])

        for params in analytics_params:

            report = Report(client, path_to_bq, date_from, date_to)
            report.get_analytics_report(params['view_id'], path_to_ga, ["General", "Goal1to4", "Goal5to8", "Goal9to12",
                                                                        "Goal13to16", "Goal17to20"])

