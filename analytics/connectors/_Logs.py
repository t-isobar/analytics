import logging
from analytics.connectors._BigQuery import BigQuery

log_error_level = 'DEBUG'
logger_name = "NEW_LOGGER"


class LogDBHandler(logging.Handler):

    def __init__(self, path_to_json, db_name, db_table):
        logging.Handler.__init__(self)
        self.bq = BigQuery(path_to_json)
        self.db_name = db_name
        self.db_table = db_table

    def emit(self, record):
        log_msg = record.msg
        log_msg = log_msg.strip()
        log_msg = log_msg.replace('\'', '\'\'')
        sql = f'INSERT INTO `{self.db_name}.{self.db_table}` VALUES ({record.levelno}, "{record.levelno}", ' \
            f'"{record.levelname}", "{log_msg}", "{record.name}")'
        self.bq.get_query(sql)


# log_db = LogDBHandler("t-isobar-bq.json", "TIsobar_Logs", "Maxidom_Letter_Logs")
#
# logging.getLogger('').addHandler(log_db)
#
# log = logging.getLogger(logger_name)
# log.setLevel(log_error_level)
