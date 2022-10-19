from connectDB import DataConn
from drivers import get_drivers
from waybills import get_waybills
from payments import get_payments_lists
from cars import get_cars
from last_date_finder import last_date, create_last_date
import json
# from rides import


def main():
    date = last_date()
    with open("./connect_config.json") as config:
        connect_data = json.load(config)
        ftp_cond = connect_data['ftp']
        idb_cond = connect_data['input_db']
        odb_cond = connect_data['output_db']
    payments = get_payments_lists(ftp_cond, date)
    print('получены платежи')
    waybills = get_waybills(ftp_cond, date)
    print('получены пл')
    with DataConn(**idb_cond) as conn:
        drivers = get_drivers(waybills, conn)
        print('получены водители')
        cars = get_cars(date, conn)
        print('получены машины')
        # rides = get_rides
    print('end')
    # create_last_date()


if __name__ == "__main__":
    # create_last_date()
    main()
