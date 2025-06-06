import os
from datetime import datetime as dt
from datetime import timedelta

import aiogram
import holidays
import pandas as pd
from aiogram.exceptions import TelegramAPIError
from dotenv import load_dotenv
from pandas.core.frame import DataFrame

from exceptions import TelegramSendMessageError
from logger import logging
from worksheets import connect_to_settings_ws

pd.options.mode.chained_assignment = None

MONTHS = {'1': f'Январь {dt.now().year}',
          '2': f'Февраль {dt.now().year}',
          '3': f'Март {dt.now().year}',
          '4': f'Апрель {dt.now().year}',
          '5': f'Май {dt.now().year}',
          '6': f'Июнь {dt.now().year}',
          '7': f'Июль {dt.now().year}',
          '8': f'Август {dt.now().year}',
          '9': f'Сентябрь {dt.now().year}',
          '10': f'Октябрь {dt.now().year}',
          '11': f'Ноябрь {dt.now().year}',
          '12': f'Декабрь {dt.now().year}'}


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
env_path = os.path.join(BASE_DIR, '.env')
load_dotenv(dotenv_path=env_path)


TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')


async def send_message(bot: aiogram.Bot, message: str):
    '''Присылает сообщение в телеграме.'''
    try:
        await bot.send_message(chat_id=TELEGRAM_CHAT_ID,
                               text=f'{message}')
    except TelegramAPIError as error:
        raise TelegramSendMessageError(error)


def non_count_engineers() -> list[str]:
    '''
    Возвращает список проектировщиков, для которых не надо делать
    премиальный расчет.
    '''
    sheet = connect_to_settings_ws()

    df = sheet.get('A1:A20')
    non_count_eng = pd.DataFrame(df[1:], columns=df[0])
    if not non_count_eng.empty:
        non_count_eng_list = non_count_eng['Не учитывать'].tolist()
        logging.info(
            f'Cписок проектировщиков, для которых не '
            f'надо делать расчет: {non_count_eng_list}')
        return non_count_eng_list
    return None


def get_list_of_engineers(df: DataFrame, colomn: str) -> list:
    '''
    Принимает датафрейм и название столбца,
    из которого брать фамилии проектировщиков.
    Возвращает список проектировщиков.
    '''
    engineers = set(df[colomn])
    non_count_eng = non_count_engineers()
    groups_of_engineers = set()
    groups = set()

    if '' in engineers:
        engineers.remove('')

    for engineer in engineers:
        if ',' in engineer:
            groups_of_engineers.add(engineer)

    for group in groups_of_engineers:
        groups.update(group.split(', '))
        engineers.remove(group)

    union_eng = engineers.union(groups)

    if not non_count_eng:
        return list(union_eng)
    unique_eng = union_eng - set(non_count_eng)
    return list(unique_eng)


def is_point(s: str) -> bool:
    '''
    Проверка столбца "Баллы".
    Если значение состоит из строки, в котрой только число, возвращает True.
    '''
    try:
        float(s)
        return True
    except ValueError:
        return False


def count_non_working_days(start_date: dt.date, end_date: dt.date) -> int:
    '''Считает количество нерабочих дней в заданном промежутке.'''
    if start_date > end_date:
        start_date, end_date = end_date, start_date

    ru_holidays = holidays.RU(years=range(start_date.year, end_date.year + 1))

    non_working_days = 0

    current_date = start_date
    while current_date <= end_date:
        if current_date.weekday() >= 5 or current_date in ru_holidays:
            non_working_days += 1
        current_date += timedelta(days=1)

    return non_working_days


def define_integer(integer: str) -> float:
    '''
    Подготавливает введенные данные для дальнейших вычислений.
    '''
    integer = integer.replace(' ', '')
    integer = integer.replace('\xa0', '')
    try:
        integer = float(integer)
    except ValueError:
        if ',' in integer:
            numbers = integer.split(',')
            integer = float(numbers[0])
        else:
            integer = 0
    return integer
