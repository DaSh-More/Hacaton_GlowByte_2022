from ftplib import FTP_TLS as FTP
from datetime import datetime, timedelta


def time_segments(start, delta: timedelta, end=datetime.today()):
    """
    Возвращает даты начиная с указанной и по конечную с указанным шагом

    Args:
        start (datetime): Начальная дата
        delta (timedelta): Шаг
        end (datetime, optional): Конечная дата. Defaults to datetime.today().

    Yields:
        datetime: Дата
    """
    while start <= end:
        yield start
        start += delta


def files_names(last_date: datetime) -> list:
    """
    Возвращает имена файлов которые надо получить
    (с последней даты по текущую с шагом 30 минут)

    Args:
        last_date(datetime): Дата начиная с которой возвращать

    Returns:
        (list): Список имен файлов
    """
    last_date = last_date.replace(second=0, minute=0)
    dates = []
    template = '%Y-%m-%d_%H-%M'
    for date in time_segments(last_date, timedelta(minutes=30)):
        dates.append(f'payment_{date.strftime(template)}.csv')
    return dates


def ftp_connect(connect_ftp):
    """
    Возвращает соединение по ftp :

    Args:
        connect_ftp (dict): Словарь с данными для подключения

    Returns:
        FTP: Объект для работы с подключением
    """
    ftp = FTP(**connect_ftp)
    ftp.prot_p()
    ftp.cwd('payments')
    return ftp


def get_payments_lists(connect_data: dict, last_date: datetime) -> list:
    """
    Данные о плетежах начиная с указанной даты

    Args:
        connect_data (dict): Данные для подключения к ftp
            (host, user, passwd)
        last_date (datetime): Дата начиная с которой надо вернуть данные

    Returns:
        list: Список с данными о плетежах
    """
    files = []

    def add_file(file):
        files.append(file.decode('utf8'))
    ftp = ftp_connect(connect_data)
    exists_files = ftp.nlst()
    # Получаем файлы с указанной даты
    for file_name in files_names(last_date):
        if file_name in exists_files:
            try:
                ftp.retrbinary('RETR ' + file_name, add_file)
            except EOFError:
                # Если ошибка подключения, переподключаемся
                ftp = ftp_connect(connect_data)
                ftp.retrbinary('RETR ' + file_name, add_file)
    data = []
    # Добавляем в словарь
    for file in files:
        data = csv2json(file, data)
    return data


def csv2json(file, data=[]):
    """
    Добавляет записи в словарь

    Args:
        file (str): Содержимое csv файла
        data (list, optional): Наполняемый список. Defaults to [].

    Returns:
        list: Список с добавленными записями
    """
    for row in file.splitlines():
        date, card_num, amount = row.split('\t')
        date = datetime.strptime(date, '%d.%m.%Y %H:%M:%S')
        data.append({'date': date, 'card_num': card_num, 'amount': amount})
    return data


def main():
    import json
    with open('./connect_config.json') as f:
        data = json.load(f)['ftp']
    print(get_payments_lists(data, datetime.now())[0])


if __name__ == "__main__":
    main()
