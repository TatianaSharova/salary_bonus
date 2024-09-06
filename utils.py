import os

import aiogram
import pandas as pd
from aiogram.exceptions import TelegramAPIError
from dotenv import load_dotenv
from gspread_formatting import *
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
EMAIL = os.getenv('EMAIL')


async def send_message(bot: aiogram.Bot, message: str):
    '''Присылает сообщение в телеграме.'''
    try:
        await bot.send_message(chat_id=TELEGRAM_CHAT_ID,
                               text=f'{message}')
    except TelegramAPIError as error:
        raise TelegramSendMessageError(error)


def non_count_engineers() -> list[str]:
    '''
    Получает список проектировщиков, для которых не надо делать премиальный расчет.
    '''
    sheet = connect_to_settings_ws()
    
    df = pd.DataFrame(sheet.get_all_records())
    if not df.empty:
        engineers = df['Не учитывать'].iloc[0].split(', ')                                #TODO избавиться от ', '
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


def count_block(row: Series, blocks: list) -> float:
    '''
    Расчитывает баллы для блок-контейнеров.
    За первый разработанный объект - 1 балл,
    за остальные - 0.5 балла.
    '''
    name = row['Наименование объекта']

    if name in blocks:
        return 0.5
    else:
        blocks.append(name)
        return 1.0


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
