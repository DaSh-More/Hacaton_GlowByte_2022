import psycopg2


class DataConn:

    class Cursor:
        """
        Объект курсора, при вызове возвращает текст запроса
        """

        def __init__(self, cursor):
            self.cursor = cursor

        def __call__(self, sql):
            self.cursor.execute(sql)
            return self.cursor.fetchall()

    def __init__(self, **connect_data):
        needs_data = ['user', 'password', 'host', 'dbname']
        self.db_info = {key: value for key, value
                        in connect_data.items() if key in needs_data}

    def __enter__(self):
        """
        Открываем подключение с базой данных.
        """
        self.conn = psycopg2.connect(**self.db_info)
        return self.Cursor(self.conn.cursor())

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Закрываем подключение.
        """
        self.conn.close()
        if exc_val:
            raise
