#!usr/bin/python3

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
        date_to_dt =datetime.strptime(date_to, "%Y-%m-%d")
        time_delta = (date_to_dt - date_from_dt).days
        if time_delta > period:
            while date_from_dt <= date_to_dt:
                if (date_to_dt - date_from_dt).days >= period-1:

                    date_range.append((datetime.strftime(date_from_dt, "%Y-%m-%d"), datetime.strftime(date_from_dt + timedelta(days=period-1), "%Y-%m-%d")))
                    date_from_dt += timedelta(days=period)

                else:
                    dif_days = (date_to_dt - date_from_dt).days

                    date_range.append((datetime.strftime(date_from_dt, "%Y-%m-%d"), datetime.strftime(date_from_dt + timedelta(days=dif_days), "%Y-%m-%d")))
                    date_from_dt += timedelta(days=dif_days+1)
            return date_range
        else:
            return [(date_from, date_to)]

