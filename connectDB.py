import psycopg2


def connect_to_DB(kwargs: dict):
    '''
    Connecting to a database with authorization data
    :param kwargs: dictionary with authorization data
    :return: psycopg2 Session
    '''
    conn = psycopg2.connect(dbname=kwargs['dbname'], user=kwargs['user'],
                            password=kwargs['password'], host=kwargs['host'])
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM main.rides')
    return cursor
