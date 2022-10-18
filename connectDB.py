import datetime

import psycopg2


class DataConn:

    def __init__(self, **kwargs):
        self.db_info = kwargs

    def __enter__(self):
        """
        Открываем подключение с базой данных.
        """
        self.conn = psycopg2.connect(dbname=self.db_info['dbname'], user=self.db_info['user'],
                                     password=self.db_info['password'], host=self.db_info['host'])
        return self.conn.cursor()

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Закрываем подключение.
        """
        self.conn.close()
        if exc_val:
            raise


# if __name__ == '__main__':
#     diction = {'dbname': 'taxi', 'user': 'etl_tech_user', 'password': 'etl_tech_user_password',
#                'host': 'de-edu-db.chronosavant.ru'}
#     with DataConn(**diction) as cursor:
#         cursor.execute('SELECT * FROM main.rides LIMIT 10')
#         for i in cursor:
#             print(i)

def get_movement_end_cancel(time):
    movements = []
    diction = {'dbname': 'taxi', 'user': 'etl_tech_user', 'password': 'etl_tech_user_password',
                   'host': 'de-edu-db.chronosavant.ru'}
    with DataConn(**diction) as cursor:
        cursor.execute("SELECT * FROM main.movement WHERE event = 'END' OR event = 'CANCEL'")
        for move in cursor:
            if move[4] > time:
                move_pars = [
                              {
                              "car_plate_num": move[1],
                              "ride": move[2],
                              "event": move[3],
                              "date": move[4]
                              }
                            ]
                movements.append(move_pars)

        return movements

