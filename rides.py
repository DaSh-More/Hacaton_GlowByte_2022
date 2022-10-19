import json
import datetime
from connectDB import DataConn


def get_movement_end_cancel(time, cursor):
    movements = []
    # ! Переписал проверку на время внутрь sql
    data_list = cursor(
        f"""
        SELECT * FROM main.movement
        WHERE (event = 'END' OR event = 'CANCEL') and dt > '{time}'
        """)

    # TODO Сделать проверку на тип события и установить правильные
    for move in data_list:
        move_pars = {
            "car_plate_num": move[1],
            "ride": move[2],
            "event": move[3],
            "date": move[4]
        }
        movements.append(move_pars)

    return movements


if __name__ == '__main__':
    with open('./connect_config.json') as f:
        data = json.load(f)['input_db']

    with DataConn(**data) as cursor:
        # data_list = cursor('SELECT * FROM main.rides LIMIT 10')
        date = datetime.datetime.today() - datetime.timedelta(days=2)
        print(*get_movement_end_cancel(date, cursor)[:3], sep='\n')
