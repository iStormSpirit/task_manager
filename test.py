import logging
import multiprocessing
import os
import shutil
import time
import warnings
from datetime import datetime
from logging.config import fileConfig
from typing import List, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup

from job import Job
from scheduler import Scheduler

warnings.filterwarnings('ignore', message='Unverified HTTPS request')

HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36\
     (HTML, like Gecko) Chrome/77.0.3865.120 Safari/537.36'}
CITIES_DICT: dict = {
    'Москва': 4368,
    'Санкт-Петербург': 4079,
    'Нью-Йорк': 7190,
    'Торонто': 7034,
    'Прага': 2952
}


def download_weather(city: str, year: str = '2022', month: str = '09') -> str:
    time.sleep(2)
    dict_temp = {}
    try:
        city_cod = CITIES_DICT[city]
    except KeyError:
        logging.exception('По этому городу информации нет')
        return 'failure city name'
    url = f'https://www.gismeteo.ru/diary/{city_cod}/{year}/{month}/'
    try:
        r = requests.get(url, headers=HEADERS, verify=False)
        soup = BeautifulSoup(r.text, 'lxml')
        columns = soup.find_all('td', {'class': 'first_in_group'})
    except requests.exceptions.ConnectionError:
        logging.error('Нет соединения с этим URL')
        return 'failure connection'
    i = 0
    list_value = []
    for col in columns:
        if i % 2 == 0:
            list_value.append(col.text)
        i = i + 1
    length = 31 - len(list_value)
    for n in range(length):
        list_value.append('300')
    dict_temp[city] = list_value
    data_temp = pd.DataFrame(
        dict_temp,
        index=[
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
            11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
            21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31
        ]
    )
    data_temp = data_temp.T
    data_temp.to_excel(f'Weather-{year}.{month}-{city}.xlsx')
    return 'success'


def get_file_weather_list() -> Tuple[str, List[str]]:
    address = os.getcwd()
    file_list = os.listdir(".")
    weather_file_list = []
    for file in file_list:
        if 'Weather' in file:
            weather_file_list.append(file)
    return address, weather_file_list


def make_dir() -> str:
    time.sleep(0)
    address, weather_file_list = get_file_weather_list()
    if os.path.exists(address + '/' + 'WEATHER_ARCHIVE'):
        os.chdir(address + '/' + 'WEATHER_ARCHIVE')
    else:
        os.mkdir('WEATHER_ARCHIVE')
        os.chdir(address + '/' + 'WEATHER_ARCHIVE')
    for file in weather_file_list:
        year_dir = file[8:12]
        if os.path.exists(
                address + '/' + 'WEATHER_ARCHIVE' + '/' + year_dir):
            continue
        else:
            os.mkdir(year_dir)
    os.chdir(address)
    return 'success'


def move_files() -> str:
    address, weather_file_list = get_file_weather_list()
    for file in weather_file_list:
        year_dir = file[8:12]
        if os.path.exists(
                address + '/' + 'WEATHER_ARCHIVE'
                + '/' + year_dir + '/' + file
        ):
            os.remove(file)
        else:
            shutil.move(
                address + '/' + file, address + '/'
                + 'WEATHER_ARCHIVE' + '/' + year_dir
            )
    return 'success'


if __name__ == '__main__':
    dt = datetime(2023, 4, 5, 6, 31, 10)
    st_time = datetime.now()
    condition = multiprocessing.Condition()
    fileConfig('logging.ini')

    J1 = Job(
        name='J1 - Скачать погоду для Москвы',
        start_at=dt,
        max_working_time=0,
        tries=3,
        func=download_weather,
        dependencies=[],
        args=['Москва', '2022', '09']
    )
    J2 = Job(
        name='J2 - Скачать погоду для Питера',
        start_at=dt,
        max_working_time=0,
        tries=3,
        func=download_weather,
        args=['Санкт-Петербург', '2022', '09']
    )
    J3 = Job(
        name='J3 - Скачать погоду для Нью-Йорка',
        start_at=dt,
        max_working_time=0,
        tries=3,
        func=download_weather,
        args=['Нью-Йорк', '2022', '09']
    )
    J4 = Job(
        name='J4 - Скачать погоду для Торонто',
        start_at=dt,
        max_working_time=0,
        tries=3,
        func=download_weather,
        args=['Торонто', '2022', '09']
    )
    J5 = Job(
        name='J5 - Скачать погоду для Праги',
        start_at=dt,
        max_working_time=0,
        tries=3,
        func=download_weather,
        args=['Прага', '2022', '09']
    )
    J6 = Job(
        name='J6 - Актуализировать папки',
        start_at=dt,
        max_working_time=0,
        tries=3,
        func=make_dir,
        dependencies=[J1, J2, J3, J4, J5],
    )
    J7 = Job(
        name='J7 - Перенести файлы',
        start_at=dt,
        max_working_time=0,
        tries=3,
        func=move_files,
        dependencies=[J6]
    )
    J8 = Job(
        name='J8 - Перенести файлы',
        start_at=dt,
        max_working_time=0,
        tries=3,
        func=move_files,
        dependencies=[J1, J2]
    )

    sc = Scheduler()
    manager_sc = sc.schedule()
    manager_sc.send((J7, J2, J5, J6, J1, J3, J4))
    manager_sc.send((J1, J2, J8))
    manager_sc.close()
    logging.info(f'Время выполнения {datetime.now() - st_time} с.')
