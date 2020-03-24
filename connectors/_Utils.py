from datetime import datetime, timedelta


def my_slice(slice_ids, limit=5, slice_list=[]):
    count = len(slice_ids)
    if count > limit:
        slice_list.append(slice_ids[:limit])
        return my_slice(slice_ids[limit:], limit, slice_list=slice_list)
    else:
        slice_list.append(slice_ids)
        return slice_list


def slice_date_on_period(date_from, date_to, period):
    date_range = []
    date_from_dt = datetime.strptime(date_from, "%Y-%m-%d")
    date_to_dt = datetime.strptime(date_to, "%Y-%m-%d")
    time_delta = (date_to_dt - date_from_dt).days
    if time_delta > period:
        while date_from_dt <= date_to_dt:
            if (date_to_dt - date_from_dt).days >= period - 1:

                date_range.append((datetime.strftime(date_from_dt, "%Y-%m-%d"),
                                   datetime.strftime(date_from_dt + timedelta(days=period - 1), "%Y-%m-%d")))
                date_from_dt += timedelta(days=period)

            else:
                dif_days = (date_to_dt - date_from_dt).days

                date_range.append((datetime.strftime(date_from_dt, "%Y-%m-%d"),
                                   datetime.strftime(date_from_dt + timedelta(days=dif_days), "%Y-%m-%d")))
                date_from_dt += timedelta(days=dif_days + 1)
        return date_range
    else:
        return [(date_from, date_to)]


def convert_data_frame_as_type(data_frame, integer_fields, float_fields, string_fields):
    columns_name = list(data_frame.columns)
    to_int_ = list(set(columns_name).intersection(set(integer_fields)))
    to_float_ = list(set(columns_name).intersection(set(float_fields)))
    to_str_ = list(set(columns_name).intersection(set(string_fields)))

    data_frame = data_frame.astype({x: "int" for x in to_int_}).astype({x: "float" for x in to_float_}) \
        .astype({x: "str" for x in to_str_})

    return data_frame


def create_fields(client_name, platform, report_dict):
    tables_with_schema = {f"{client_name}_{platform}_{report_name}": report_dict[report_name]['fields']
                          for report_name in list(report_dict.keys())}

    string_fields = list(set([key for values in report_dict.values() for key, value in values['fields'].items()
                              if value == "STRING"]))
    integer_fields = list(set([key for values in report_dict.values() for key, value in values['fields'].items()
                               if value == "INTEGER"]))
    float_fields = list(set([key for values in report_dict.values() for key, value in values['fields'].items()
                             if value == "FLOAT"]))

    return tables_with_schema, string_fields, integer_fields, float_fields


def create_fields_ga(client_name, platform, report_dict):
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


def expand_dict(data_to_expand, dict_with_keys, dict_with_data):
    if isinstance(data_to_expand, dict):
        for key, value in data_to_expand.items():
            if isinstance(value, str):
                if key in dict_with_keys.keys():
                    dict_with_data[dict_with_keys[key]] = value
                else:
                    dict_with_data[key] = value
            else:
                dict_with_data = expand_dict(value, dict_with_keys, dict_with_data)
    elif isinstance(data_to_expand, list):
        for element_of_list in data_to_expand:
            dict_with_data = expand_dict(element_of_list, dict_with_keys, dict_with_data)
    return dict_with_data

