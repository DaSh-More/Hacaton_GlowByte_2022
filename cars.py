import json
import datetime
from connectDB import DataConn


def new_cars(time, cursor):
    """
    Возвращает словарь с информацией о машинах 
    у которых дата регистрации или техосмотра позже указанной

    Args:
        time (datetime.datetime): Время начиная с котороко нужны машины
        cursor (DataConn.Cursor): Объект подключения к базе

    Returns:
        dict: Словарь с данными о машинах
            {'plate_num': {
                plate_num": "",
                "model": "",
                "revision_dt": "",
                "register_dt": "",
                "finished_flg": ""
                }
            }
    """
    cars_dict = {}
    cars = cursor(f"""
                     SELECT * FROM main.car_pool
                     WHERE revision_dt > '{time}'
                        OR register_dt > '{time}'
                     """)
    for car in cars:
        cars_dict[car[0]] = {
            "plate_num": car[0],
            "model": car[1],
            "revision_dt": car[2],
            "register_dt": car[3],
            "finished_flg": car[4],
        }
    return cars_dict


def main():
    from pprint import pprint
    with open('./connect_config.json') as f:
        data = json.load(f)['input_db']
    date = datetime.datetime.today() - datetime.timedelta(days=2)
    with DataConn(**data) as cursor:
        pprint(new_cars(date, cursor))


if __name__ == "__main__":
    main()
