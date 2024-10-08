import os
from datetime import datetime as dt
from datetime import timedelta

import aiogram
import holidays
import pandas as pd
from aiogram.exceptions import TelegramAPIError
from dotenv import load_dotenv
from pandas.core.frame import DataFrame
from pandas.core.series import Series

from exceptions import TelegramSendMessageError
from worksheets import connect_to_settings_ws

pd.options.mode.chained_assignment = None


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
    Возвращает список проектировщиков, для которых не надо делать премиальный расчет.
    '''
    sheet = connect_to_settings_ws()
    
    df = sheet.get('A1:A20')
    non_count_eng = pd.DataFrame(df[1:], columns=df[0])
    if not non_count_eng.empty:
        engineers = non_count_eng['Не учитывать'].tolist()
        return engineers
    else:
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
    else:
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
