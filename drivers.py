from connectDB import DataConn
import json
import datetime


with open('connect_config.json') as f:
    data = json.load(f)['input_db']


def make_to_dic(dic, waybills):
    if dic[0] in waybills:
        start_date = datetime.datetime.strptime(
            waybills[dic[0]]['period']['start'], "%Y-%m-%d %H:%M:%S")
        end_date = datetime.datetime.strptime(
            waybills[dic[0]]['period']['end'], "%Y-%m-%d %H:%M:%S")
    else:
        start_date = end_date = 0
    return {
        "start_date": start_date,
        "end_date": end_date,
        "last_name": dic[2],
        "first_name": dic[1],
        "middle_name": dic[3],
        "birth_dt": "null",
        "card_num": dic[5],
        "driver_license_num": dic[0],
        "driver_license_dt": dic[4],
        "deleted_flag": "F"
    }


def get_drivers(waybills, cursor):
    drivers = cursor('SELECT * FROM main.drivers')
    return [make_to_dic(driver, waybills) for driver in drivers]


if __name__ == "__main__":
    from pprint import pprint
    from waybills import get_waybills
    with open('./connect_config.json') as f:
        data = json.load(f)
        data_ftp = data['ftp']
        data_db = data['input_db']
    date = datetime.datetime.now() - datetime.timedelta(hours=6)
    waybills = get_waybills(data_ftp, date)
    with DataConn(**data_db) as conn:
        pprint(get_drivers(waybills, conn))
