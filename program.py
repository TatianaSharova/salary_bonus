import time
import os
import asyncio
import aiogram
from aiogram.exceptions import TelegramAPIError
from datetime import datetime as dt

from dotenv import load_dotenv

import gspread
import pandas as pd
from gspread.worksheet import Worksheet
from gspread_formatting import *
from pandas.core.frame import DataFrame
from pandas.core.series import Series

from exceptions import TooManyRequestsApiError
from worksheets import (connect_to_project_archive, connect_to_settings_ws,
                        send_quarter_data_to_spreadsheet,
                        send_data_to_spreadsheet)

pd.options.mode.chained_assignment = None
from counting_points import (check_amount_directions, check_authors,
                             check_cultural_heritage, check_filled_projects,
                             check_net, check_sot_skud, check_spend_time,
                             check_square)
from quaterly_points import calculate_quarter

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
env_path = os.path.join(BASE_DIR, '.env')
load_dotenv(dotenv_path=env_path)


creds_path = os.path.join(BASE_DIR, 'creds.json')

TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')

async def send_message(bot: aiogram.Bot, message: str):
    '''Присылает сообщение в телеграме.'''
    try:
        await bot.send_message(chat_id=TELEGRAM_CHAT_ID,
                               text=f'{message}')
    except TelegramAPIError:
        pass

def non_count_engineers() -> list[str]:
    '''
    Получает список инженеров, для которых не надо делать премиальный расчет.
    '''
    sheet = connect_to_settings_ws()
    
    df = pd.DataFrame(sheet.get_all_records())
    if not df.empty:
        engineers = df['Не учитывать'].iloc[0].split(', ')                                #TODO избавиться от ', '
        return engineers
    else:
        return None


def get_list_of_engineers(df: DataFrame) -> list:
    '''Возвращает список инженеров.'''
    engineers = set(df['Разработал'])
    non_count_eng = non_count_engineers()
    groups_of_engineers = set()
    groups = set()

    if '' in engineers:
        engineers.remove('')

    for engineer in engineers:
        if ',' in engineer:
            groups_of_engineers.add(engineer)

    for group in groups_of_engineers:
        groups.update(group.split(','))
        engineers.remove(group)
    
    engineers.union(groups)

    if not non_count_eng:
        return list(engineers)
    else:
        unique_eng = engineers - set(non_count_eng)
        return list(unique_eng)


def count_points(row: Series, df: DataFrame) -> int:
    '''
    Считает и возвращает сумму полученных баллов.
    Если подсчет произвести невозможно, то возвращает
    строку-предупреждение об этом.
    '''
    points = 0
    filled_project = check_filled_projects(row)
    if not filled_project:
        return 'Необходимо заполнить данные для расчёта'
    if 'блок-контейнер' in row['Тип объекта'].strip().lower():
        return 1
    try:
        complexity = int(row['Сложность'])
    except ValueError:
        return 'Сложность объекта заполнена некорректно'

    points += check_amount_directions(complexity, row['Количество направлений'])
    points += check_square(complexity, row)
    points += check_sot_skud(row)
    points += check_cultural_heritage(row)
    points += check_net(row)
    points = round(points/check_authors(row['Разработал']),1)
    points = check_spend_time(row, points, df)

    return points


def main_func(engineers: list[str], df: DataFrame) -> None:
    '''
    Собирает данные из архива проектов, производит расчет баллов
    и отправляет полученные данные в новую таблицу.
    '''
    for engineer in engineers:
        print(engineer)
        engineer_projects = df.loc[df['Разработал'].str.contains(f'{engineer}')].reset_index(drop=True)
        engineer_projects["Дедлайн"] = ''
        engineer_projects["Баллы"] = engineer_projects.apply(count_points, axis=1, args=(engineer_projects,))
        send_data_to_spreadsheet(engineer_projects, engineer)

        engineer_projects_filtered = engineer_projects[[
            'Шифр (ИСП)', 'Разработал', 'Дата начала проекта', 'Дата окончания проекта', 'Баллы'
        ]].loc[engineer_projects['Баллы'] != 'Необходимо заполнить данные для расчёта']

        if not engineer_projects_filtered.empty:
            quarters = calculate_quarter(engineer_projects_filtered)
            send_quarter_data_to_spreadsheet(quarters, engineer)
        

async def main():
    gc = gspread.service_account(filename=creds_path)
    bot = aiogram.Bot(token=TELEGRAM_TOKEN)

    worksheet = connect_to_project_archive()

    df = pd.DataFrame(worksheet.get_all_records())

    if not df.empty:
        list_of_engineers = get_list_of_engineers(df)
        if list_of_engineers != []:
            try:
                salary_bonus = main_func(list_of_engineers, df)
                await send_message(bot, 'Расчет баллов успешно выполнен.')
            except gspread.exceptions.APIError as error:
                raise TooManyRequestsApiError(error)
            except Exception as error:
                await send_message(bot, f'Ошибка: {error}')
    
    await bot.session.close()
    

if __name__ == "__main__":
    asyncio.run(main())
