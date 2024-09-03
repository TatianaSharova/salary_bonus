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
                        send_project_data_to_spreadsheet,
                        send_adj_data_to_spreadsheet)

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



def count_points(row: Series, df: DataFrame, blocks: list) -> int:
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
        return count_block(row, blocks)
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


def points_for_adjusting(row: Series, engineer: str):
    '''
    Расчитывает баллы за корректировки: 0.3 от баллов за готовый проект, не считая дедлайн.
    '''
    points = 0
    filled_project = check_filled_projects(row)

    if engineer in row['Разработал']:
        return 'Баллы за корректировки своих проектов не расчитываются'

    if not filled_project:
        return 'Необходимо заполнить данные для расчёта'
    if 'блок-контейнер' in row['Тип объекта'].strip().lower():
        return 0.3
    
    try:
        complexity = int(row['Сложность'])
    except ValueError:
        return 'Сложность объекта заполнена некорректно'

    points += check_amount_directions(complexity, row['Количество направлений'])
    points += check_square(complexity, row)
    points += check_sot_skud(row)
    points += check_cultural_heritage(row)
    points += check_net(row)
    points = round(points/check_authors(row['Корректировки']),1)

    return points*0.3



def count_points_for_adjusting(df: DataFrame) -> Worksheet:
    '''
    Расчитывает баллы за корректировки и отсылает данные в таблицу "Премирование".
    '''
    eng_for_adj = get_list_of_engineers(df, colomn='Корректировки')
    
    if eng_for_adj != []:
        for engineer in eng_for_adj:
            adjusting_projects = df.loc[df['Корректировки'].str.contains(f'{engineer}')].reset_index(drop=True)
            adjusting_projects["Баллы"] = adjusting_projects.apply(points_for_adjusting, axis=1, args=(adjusting_projects, engineer))
            adjusting_projects_small = adjusting_projects[['Наименование объекта', 'Шифр (ИСП)', 'Разработал', 'Баллы',
                    'Корректировки']]
            send_adj_data_to_spreadsheet(adjusting_projects_small, engineer)


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


def process_data(engineers: list[str], df: DataFrame) -> None:
    '''
    Собирает данные из архива проектов, производит расчет баллов
    и отправляет полученные данные в новую таблицу.
    '''
    for engineer in engineers:
        print(engineer)
        engineer_projects = df.loc[df['Разработал'].str.contains(f'{engineer}')].reset_index(drop=True)
        engineer_projects["Дедлайн"] = ''
        blocks = []
        engineer_projects["Баллы"] = engineer_projects.apply(count_points, axis=1, args=(engineer_projects, blocks))
        send_project_data_to_spreadsheet(engineer_projects, engineer)


        # engineer_projects_filtered = engineer_projects[[
        #     'Шифр (ИСП)', 'Разработал', 'Дата начала проекта', 'Дата окончания проекта', 'Баллы'
        # ]].loc[engineer_projects['Баллы'] != 'Необходимо заполнить данные для расчёта']

        engineer_projects_filt = engineer_projects[engineer_projects['Баллы'].apply(is_point)]

        engineer_projects_filtered = engineer_projects_filt[[
            'Шифр (ИСП)', 'Разработал', 'Дата начала проекта', 'Дата окончания проекта', 'Баллы'
        ]]


        if not engineer_projects_filtered.empty:
            quarters = calculate_quarter(engineer_projects_filtered)
            send_quarter_data_to_spreadsheet(quarters, engineer)
        time.sleep(20)
    
    count_points_for_adjusting(df)
        

async def main():
    gc = gspread.service_account(filename=creds_path)
    bot = aiogram.Bot(token=TELEGRAM_TOKEN)
    
    try:
        worksheet = connect_to_project_archive()
    except gspread.exceptions.SpreadsheetNotFound:
        await send_message(bot, 'Ошибка: таблица "Таблица проектов" не найдена.\n'
                           'Возможно название было сменено.')

    df = pd.DataFrame(worksheet.get_all_records(numericise_ignore=['all']))

    if not df.empty:
        list_of_engineers = get_list_of_engineers(df, colomn='Разработал')
        if list_of_engineers != []:
            try:
                salary_bonus = process_data(list_of_engineers, df)

                await send_message(bot, 'Расчет баллов успешно выполнен.')
            except gspread.exceptions.APIError as error:
                await send_message(bot, 'Ошибка: слишком много запросов к API Google.')
                raise TooManyRequestsApiError(error)
            # except Exception as error:
                # await send_message(bot, f'Ошибка: {error}')
    
    await bot.session.close()
    

if __name__ == "__main__":
    asyncio.run(main())
