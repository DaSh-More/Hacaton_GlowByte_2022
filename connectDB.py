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

def get_movement_end_cancel(time):
    movements = []
    diction = {'dbname': 'taxi', 'user': 'etl_tech_user', 'password': 'etl_tech_user_password',
                   'host': 'de-edu-db.chronosavant.ru'}
    with DataConn(**diction) as cursor:
        cursor.execute("SELECT * FROM main.movement WHERE event = 'END' OR event = 'CANCEL'")
        for move in cursor:
            if move[4] > time:
                move_pars = {
                              "car_plate_num": move[1],
                              "ride": move[2],
                              "event": move[3],
                              "date": move[4]
                              }
                movements.append(move_pars)
        print(move)

        return movements

# get_movement_end_cancel(datetime.datetime.today() - datetime.timedelta(2))

def get_rides(cursor):
    diction = {'dbname': 'taxi', 'user': 'etl_tech_user', 'password': 'etl_tech_user_password',
               'host': 'de-edu-db.chronosavant.ru'}
    # with DataConn(**diction) as cursor:
    cursor.execute("SELECT * FROM main.rides WHERE ride_id in (SELECT ride FROM main.movement WHERE event in ('END', 'CANCEL'))")
    curs = cursor.fetchall()
    curs_dict = {}
    for i in curs:
        curs_dict[i[0]] = i

    cursor.execute("SELECT * FROM main.movement WHERE ride in (SELECT ride FROM main.movement WHERE event in ('END', 'CANCEL'))")


    movements = []
    for move in cursor:
        move_pars = {
                      "ride_id": move[2],
                        "point_from_txt":curs_dict[move[2]][4],
                        "point_to_txt":curs_dict[move[2]][5],
                         "distance_val":curs_dict[move[2]][6],
                          "price_amt":curs_dict[move[2]][7],
                          "client_phone_num":curs_dict[move[2]][2],
                        "car_plate_num": move[1],
                         "driver_pers_num":"",
                          "ride_arrival_dt":"",
                          "ride_end_dt":"",
                          "ride_start_dt":""
                      }
        if move[3] == 'READY':
            move_pars["ride_arrival_dt"] = move[4]
        elif move[3] == 'END' or move[3] == 'CANCEL':
            move_pars["ride_end_dt"] = move[4]
        elif move[3] == 'BEGIN':
            move_pars["ride_start_dt"] = move[4]
        movements.append(move_pars)
    return movements




diction = {'dbname': 'taxi', 'user': 'etl_tech_user', 'password': 'etl_tech_user_password',
               'host': 'de-edu-db.chronosavant.ru'}
with DataConn(**diction) as cursor:
    print(get_rides(cursor)[0])


##################################################
####################
                    #################
                                     #############