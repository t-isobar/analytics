from datetime import datetime, timedelta


class Utils:
    def __init__(self):
        pass

    def my_slice(self, slice_ids, limit=5, slice_list=[]):
        count = len(slice_ids)
        if count > limit:
            slice_list.append(slice_ids[:limit])
            return self.my_slice(slice_ids[limit:], limit, slice_list=slice_list)
        else:
            slice_list.append(slice_ids)
            return slice_list

    def slice_date_on_period(self, date_from, date_to, period):
        date_range = []
        date_from_dt = datetime.strptime(date_from, "%Y-%m-%d")
        date_to_dt = datetime.strptime(date_to, "%Y-%m-%d")
        time_delta = (date_to_dt - date_from_dt).days
        if time_delta > period:
            while date_from_dt <= date_to_dt:
                if (date_to_dt - date_from_dt).days >= period-1:

                    date_range.append((datetime.strftime(date_from_dt, "%Y-%m-%d"),
                                       datetime.strftime(date_from_dt + timedelta(days=period-1), "%Y-%m-%d")))
                    date_from_dt += timedelta(days=period)

                else:
                    dif_days = (date_to_dt - date_from_dt).days

                    date_range.append((datetime.strftime(date_from_dt, "%Y-%m-%d"),
                                       datetime.strftime(date_from_dt + timedelta(days=dif_days), "%Y-%m-%d")))
                    date_from_dt += timedelta(days=dif_days+1)
            return date_range
        else:
            return [(date_from, date_to)]

    def convert_data_frame_as_type(self, data_frame, integer_fields, float_fields, string_fields):
        columns_name = list(data_frame.columns)
        to_int_ = list(set(columns_name).intersection(set(integer_fields)))
        to_float_ = list(set(columns_name).intersection(set(float_fields)))
        to_str_ = list(set(columns_name).intersection(set(string_fields)))

        data_frame = data_frame.astype({x: "int" for x in to_int_}).astype({x: "float" for x in to_float_})\
            .astype({x: "str" for x in to_str_})

        return data_frame

    def create_fields(self, client_name, platform, report_dict):
        tables_with_schema = {f"{client_name}_{platform}_{report_name}": report_dict[report_name]['fields']
                              for report_name in list(report_dict.keys())}

        string_fields = list(set([key for values in report_dict.values() for key, value in values['fields'].items()
                                  if value == "STRING"]))
        integer_fields = list(set([key for values in report_dict.values() for key, value in values['fields'].items()
                                   if value == "INTEGER"]))
        float_fields = list(set([key for values in report_dict.values() for key, value in values['fields'].items()
                                 if value == "FLOAT"]))

        return tables_with_schema, string_fields, integer_fields, float_fields

    def create_fields_ga(self, client_name, platform, report_dict):
        tables_with_schema = {f"{client_name}_{platform}_{report_name}":
                                       dict(list(report_dict[report_name]['metrics'].items()) +
                                            list(report_dict[report_name]['dimensions'].items())) for report_name
                                   in list(report_dict.keys())}

        string_fields = list(set([key for values in report_dict.values() for key, value in
                                  dict(list(values['metrics'].items()) + list(values['dimensions'].items())).items()
                                  if value == "STRING"]))
        integer_fields = list(set([key for values in report_dict.values() for key, value in
                                   dict(list(values['metrics'].items()) + list(values['dimensions'].items())).items()
                                   if value == "INTEGER"]))
        float_fields = list(set([key for values in report_dict.values() for key, value in
                                 dict(list(values['metrics'].items()) + list(values['dimensions'].items())).items()
                                 if value == "FLOAT"]))

        return tables_with_schema, string_fields, integer_fields, float_fields


