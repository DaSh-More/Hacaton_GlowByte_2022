import json
from bs4 import BeautifulSoup
from ftplib import FTP_TLS as FTP
from datetime import datetime, timedelta


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
    ftp.cwd('waybills')
    return ftp


def get_waybills(connect_ftp, time):
    files = []

    def add_file(file):
        soup = BeautifulSoup(file, 'xml')
        files.append(soup)

    ftp = ftp_connect(connect_ftp)

    file_names = ftp.nlst()[::-1]

    for f in file_names:
        try:
            ftp.retrbinary('RETR ' + f, add_file)
        except EOFError:
            # Если ошибка подключения, переподключаемся
            ftp = ftp_connect(connect_ftp)
            ftp.retrbinary('RETR ' + f, add_file)

        # Определяем дату
        date = datetime.strptime(
            files[-1].waybill.period.find('stop').text, "%Y-%m-%d %H:%M:%S")

        # Если дата слишком ранняя, прекращаем
        if date <= time:
            break
    ftp.close()
    return {file['driver']['license']: file for file in map(to_di, files)}


def to_di(dic):
    return {
        "number": dic.waybill.get('number'),
        "issuedt": dic.waybill.get('issuedt'),
        "car": {
            "plate_num": dic.waybill.car.text,
            "model": dic.waybill.model.text
        },
        "driver": {
            "name": dic.waybill.driver.find('name').text,
            "license": dic.waybill.driver.find('license').text,
            "validto": dic.waybill.driver.find('validto').text
        },
        "period": {
            "start": dic.waybill.period.find("start").text,
            "end": dic.waybill.period.find("stop").text
        }
    }


if __name__ == "__main__":
    with open('./connect_config.json') as f:
        data = json.load(f)['ftp']
    print(get_waybills(data, datetime.today() - timedelta(days=1)))
