#!usr/bin/python3
import sys
sys.path.insert(1, '/home/isobar/traffic-report/json_files')

from google.cloud import bigquery
import time


class BigQuery:
    def __init__(self, path_to_json):
        self.client = bigquery.Client.from_service_account_json(path_to_json)
        self.project = self.client.project

    def get_datasets(self):
        datasets = [dataset.dataset_id for dataset in list(self.client.list_datasets())]
        return datasets

    def get_tables(self, dataset_id):
        dataset_ref = self.client.dataset(dataset_id)
        tables_list = list(self.client.list_tables(dataset_ref))
        tables = [table.table_id for table in tables_list]
        return tables

    def check_or_create_dataset(self, dataset_id):
        datasets = self.get_datasets()
        if dataset_id not in datasets:
            self.create_dataset(dataset_id)

    def check_or_create_tables(self, dict_of_table_ids, dataset_id):
        tables = self.get_tables(dataset_id)
        tables_to_create = list(set(list(dict_of_table_ids.keys())).difference(tables))
        if tables_to_create != []:
            for table_id in tables_to_create:
                self.create_table(dataset_id, table_id, dict_of_table_ids[table_id])
            time.sleep(30)

    def __create_schema(self, schema_dict):
        schema = []
        for key, value in schema_dict.items():
            schema.append(bigquery.SchemaField(key, value))
        return schema

    def create_dataset(self, dataset_id):
        dataset_ref = self.client.dataset(dataset_id)
        dataset = bigquery.Dataset(dataset_ref)
        self.client.create_dataset(dataset)

    def delete_dataset(self, dataset_id, delete_contents=False):
        dataset_ref = self.client.dataset(dataset_id)
        self.client.delete_dataset(dataset_ref, delete_contents)

    def create_table(self, dataset_id, table_id, schema_dict):
        dataset_ref = self.client.dataset(dataset_id)
        schema = self.__create_schema(schema_dict)
        table_ref = dataset_ref.table(table_id)
        table = bigquery.Table(table_ref, schema=schema)
        self.client.create_table(table)

    def insert_rows(self, dataset_id, table_id, list_of_tuples):
        table_ref = self.client.dataset(dataset_id).table(table_id)
        table = self.client.get_table(table_ref)
        slice_data = self.__slice(list_of_tuples, limit=3000, slice_list=[])
        #TODO: Проверка на размер данных и автоматический подбор нужного  limit
        for one_slice in slice_data:
            self.client.insert_rows(table, one_slice)

    def insert_data_frame(self, dataset_id, table_id, data_frame):
        table_ref = self.client.dataset(dataset_id).table(table_id)
        table = self.client.get_table(table_ref)
        self.client.insert_rows_from_dataframe(table, data_frame)

    def insert_json(self, dataset_id, table_id, list_of_json):
        table_ref = self.client.dataset(dataset_id).table(table_id)
        table = self.client.get_table(table_ref)
        self.client.insert_rows_json(table, list_of_json, skip_invalid_rows =True)

    def get_table_shema(self, dataset_id, table_id):
        dataset_ref = self.client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)
        table = self.client.get_table(table_ref)
        return table.schema

    def __slice(self, slice_data, limit=100, slice_list=[]):
        count = len(slice_data)
        if count > limit:
            slice_list.append(slice_data[:limit])
            return self.__slice(slice_data[limit:], limit, slice_list=slice_list)
        else:
            slice_list.append(slice_data)
            return slice_list

    def get_table_num_rows(self, dataset_id, table_id):
        dataset_ref = self.client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)
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

    def delete_and_insert(self, data_frame, integer_fields, float_fields, string_fields, dataset_id, table_id, **kwargs):
        condition = ' AND '.join([f"{key} IN {tuple(value)}" for key, value in kwargs.items()])
        sql = f"DELETE FROM `{dataset_id}.{table_id}` WHERE {condition}"
        self.get_query(sql)

        result = self.data_to_insert(data_frame, integer_fields, float_fields, string_fields, dataset_id, table_id)
        return result

    def insert_difference(self, dataframe_for_insert, integer_fields, float_fields, string_fields, dataset_id, table_id, db_key, df_key):
        dataframe_from_db = self.get_select_query(f"SELECT * FROM `{dataset_id}.{table_id}` WHERE {db_key} != ''")
        df_to_insert = dataframe_for_insert[~(dataframe_for_insert[df_key].isin(dataframe_from_db[db_key].tolist()))]

        self.data_to_insert(df_to_insert, integer_fields, float_fields, string_fields, dataset_id, table_id)

    def data_to_insert(self, data_frame, integer_fields, float_fields, string_fields, dataset_ID, table_ID):
        columns_name = list(data_frame.columns)
        to_int_ = list(set(columns_name).intersection(set(integer_fields)))
        to_float_ = list(set(columns_name).intersection(set(float_fields)))
        to_str_ = list(set(columns_name).intersection(set(string_fields)))

        data_frame = data_frame.astype({x: "int" for x in to_int_}).astype({x: "float" for x in to_float_}).astype({x: "str" for x in to_str_})
        data_frame_T = data_frame.T
        data_frame_T_dict = data_frame_T.to_dict()
        if data_frame_T_dict != {}:
            total = list(data_frame_T_dict.values())
            sl_list = self.__slice(total, 10000, [])
            for sl in sl_list:
                self.insert_json(dataset_ID, table_ID, sl)