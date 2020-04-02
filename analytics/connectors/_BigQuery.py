from google.cloud import bigquery
import time
from analytics.connectors._Utils import convert_data_frame_as_type, my_slice


class BigQuery:
    def __init__(self, path_to_json):
        self.path_to_json = path_to_json
        self.client = bigquery.Client.from_service_account_json(path_to_json)
        self.project = self.client.project

    def get_data_sets(self):
        data_sets = [data_set.dataset_id for data_set in list(self.client.list_datasets())]
        return data_sets

    def get_tables(self, data_set_id):
        data_set_ref = self.client.dataset(data_set_id)
        tables_list = list(self.client.list_tables(data_set_ref))
        tables = [table.table_id for table in tables_list]
        return tables

    def check_or_create_data_set(self, data_set_id):
        data_sets = self.get_data_sets()
        if data_set_id not in data_sets:
            self.create_data_set(data_set_id)

    def check_or_create_tables(self, dict_of_table_ids, data_set_id):
        tables = self.get_tables(data_set_id)
        tables_to_create = list(set(list(dict_of_table_ids.keys())).difference(tables))
        if tables_to_create != []:
            for table_id in tables_to_create:
                self.create_table(data_set_id, table_id, dict_of_table_ids[table_id])
            time.sleep(30)

    def __create_schema(self, schema_dict):
        schema = []
        for key, value in schema_dict.items():
            schema.append(bigquery.SchemaField(key, value))
        return schema

    def create_data_set(self, data_set_id):
        data_set_ref = self.client.dataset(data_set_id)
        data_set = bigquery.Dataset(data_set_ref)
        self.client.create_dataset(data_set)

    def delete_data_set(self, data_set_id, delete_contents=False):
        data_set_ref = self.client.dataset(data_set_id)
        self.client.delete_dataset(data_set_ref, delete_contents)

    def create_table(self, data_set_id, table_id, schema_dict):
        data_set_ref = self.client.dataset(data_set_id)
        schema = self.__create_schema(schema_dict)
        table_ref = data_set_ref.table(table_id)
        table = bigquery.Table(table_ref, schema=schema)
        self.client.create_table(table)

    def insert_rows(self, data_set_id, table_id, list_of_tuples):
        table_ref = self.client.dataset(data_set_id).table(table_id)
        table = self.client.get_table(table_ref)
        slice_data = my_slice(list_of_tuples, limit=3000)
        #TODO: Проверка на размер данных и автоматический подбор нужного  limit
        for one_slice in slice_data:
            self.client.insert_rows(table, one_slice)

    def insert_data_frame(self, data_set_id, table_id, data_frame):
        table_ref = self.client.dataset(data_set_id).table(table_id)
        table = self.client.get_table(table_ref)
        self.client.insert_rows_from_dataframe(table, data_frame)

    def insert_json(self, data_set_id, table_id, list_of_json):
        table_ref = self.client.dataset(data_set_id).table(table_id)
        table = self.client.get_table(table_ref)
        try:
            self.client.insert_rows_json(table, list_of_json, skip_invalid_rows=True)
        except ConnectionError:
            self.client = bigquery.Client.from_service_account_json(self.path_to_json)
            self.client.insert_rows_json(table, list_of_json, skip_invalid_rows=True)

    def get_table_schema(self, data_set_id, table_id):
        data_set_ref = self.client.dataset(data_set_id)
        table_ref = data_set_ref.table(table_id)
        table = self.client.get_table(table_ref)
        return table.schema

    def get_table_num_rows(self, data_set_id, table_id):
        data_set_ref = self.client.dataset(data_set_id)
        table_ref = data_set_ref.table(table_id)
        table = self.client.get_table(table_ref)
        return table.num_rows

    def get_query(self, sql):
        query_job = self.client.query(sql, location='US')
        result = []
        for row in query_job:
            result.append(list(row))
        return result

    def get_select_query(self, sql):
        query_job = self.client.query(sql, location='US')
        return query_job.to_dataframe()

    def delete_and_insert(self, data_frame, integer_fields, float_fields, string_fields, data_set_id, table_id,
                          **kwargs):
        condition = ' AND '.join([f"{key} IN {tuple(value)}" for key, value in kwargs.items()])
        sql = f"DELETE FROM `{data_set_id}.{table_id}` WHERE {condition}"
        self.get_query(sql)

        self.data_to_insert(data_frame, integer_fields, float_fields, string_fields, data_set_id, table_id)

    # def get_difference(self, data_set_id, table_id, db_key):
    #     data_frame_from_db = self.get_select_query(f"SELECT * FROM `{data_set_id}.{table_id}` WHERE {db_key} != ''")
    #     data_frame_from_db['']

    def insert_difference(self, data_frame_for_insert, integer_fields, float_fields, string_fields, data_set_id,
                          table_id, db_key, df_key):
        data_frame_from_db = self.get_select_query(f"SELECT * FROM `{data_set_id}.{table_id}` WHERE {db_key} != ''")
        df_to_insert = data_frame_for_insert[~(data_frame_for_insert[df_key].isin(data_frame_from_db[db_key].tolist()))]

        self.data_to_insert(df_to_insert, integer_fields, float_fields, string_fields, data_set_id, table_id)

    def data_to_insert(self, data_frame, integer_fields, float_fields, string_fields, data_set_id, table_id):
        if not data_frame.empty:
            data_frame = convert_data_frame_as_type(data_frame, integer_fields, float_fields, string_fields)

            data_frame_t = data_frame.T
            data_frame_t_dict = data_frame_t.to_dict()
            if data_frame_t_dict != {}:
                total = list(data_frame_t_dict.values())
                sl_list = my_slice(total, 10000)
                for sl in sl_list:
                    self.insert_json(data_set_id, table_id, sl)
        else:
            return "DataFrame is empty"
