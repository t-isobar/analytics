import os, json, re
from datetime import datetime, timedelta
from analytics.connectors.mainReport import Report
from analytics.connectors import access_data

date_from = datetime.strftime(datetime.today() - timedelta(days=1), "%d/%m/%Y")
date_to = datetime.strftime(datetime.today() - timedelta(days=1), "%d/%m/%Y")

clients = json.load(open(os.path.join(os.path.split(os.getcwd())[0], "json_files", "clients.json"), "r"))
for client, placement in clients.items():
    calltouch_params = placement.get('calltouch', None)
    if calltouch_params:
        project = placement['bigquery']['project']
        path_to_bq = os.path.join(access_data.path_to_json,
                                  access_data.name_json_files['project'][project]['path_to_bq'])

        for params in calltouch_params:
            site_name_re = re.sub('[.-]', '_', params['name'])
            token = access_data.Calltouch[params['site_id']]
            report = Report(client, path_to_bq, date_from, date_to)
            report.get_calltouch_report(params['site_id'], token, site_name_re)
