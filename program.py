from datetime import datetime as dt

import gspread
import pandas as pd
from gspread.worksheet import Worksheet
from gspread_formatting import *
from pandas.core.frame import DataFrame

from exceptions import TooManyRequestsApiError

pd.options.mode.chained_assignment = None
from counting_points import (check_amount_directions, check_authors,
                             check_cultural_heritage, check_filled_projects,
                             check_net, check_sot_skud, check_spend_time,
                             check_square, set_project_complexity)
from quaterly_points import calculate_quarter

def non_count_engineers() -> list[str]:
    '''
    Получает список инженеров, для которых не надо делать премиальный расчет.
    '''
    try:
        sheet = gc.open(f'Премирование{dt.now().year}').worksheet(f'Настройки')
    except gspread.exceptions.SpreadsheetNotFound:
        sh = gc.create(f'Премирование{dt.now().year}')
        sh.share('kroxxatt@gmail.com', perm_type='user', role='writer', notify=True)        #TODO с кем шерить доступ
        sheet = sh.add_worksheet(title='Настройки', rows=100, cols=20)
    except gspread.exceptions.WorksheetNotFound:
        sh = gc.open(f'Премирование{dt.now().year}')
        sheet = sh.add_worksheet(title='Настройки', rows=100, cols=20)
    
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


def count_points(row: DataFrame) -> int:
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
    
    complexity = set_project_complexity(row)
    points += check_amount_directions(complexity, row['Количество направлений'])
    points += check_amount_directions(complexity, row['Другие АПТ (количество направлений)'])
    points += check_square(complexity, row)
    points += check_sot_skud(row)
    points += check_cultural_heritage(row)
    points += check_net(row)
    points = round(points/check_authors(row['Разработал']),1)
    points = check_spend_time(row, points)
    return points


def send_data_to_spreadsheet(df: DataFrame, engineer: str):
    '''
    Отправляет данные с баллами в таблицу "Премирование".
    '''
    try:
        sheet = gc.open(f'Премирование{dt.now().year}').worksheet(f'{engineer}')
    except gspread.exceptions.SpreadsheetNotFound:
        sh = gc.create(f'Премирование{dt.now().year}')
        sh.share('kroxxatt@gmail.com', perm_type='user', role='writer', notify=True)
        sheet = sh.add_worksheet(title=f'{engineer}', rows=200, cols=20)
    except gspread.exceptions.WorksheetNotFound:
        sh = gc.open(f'Премирование{dt.now().year}')
        sheet = sh.add_worksheet(title=f'{engineer}', rows=200, cols=20)

    
    eng_small = df[['Страна', 'Наименование объекта', 'Шифр (ИСП)', 'Разработал', 'Баллы',
                    'Дата начала проекта', 'Дата окончания проекта']]
    
    sheet.update([eng_small.columns.values.tolist()] + eng_small.values.tolist())
#     time.sleep(30)
#     set_column_width(sheet, 'A', 100)
#     set_column_width(sheet, 'B', 400)
#     set_column_width(sheet, 'C', 200)
#     set_column_width(sheet, 'D', 150)
#     set_column_width(sheet, 'E', 150)
#     set_column_width(sheet, 'F', 150)
#     set_column_width(sheet, 'G', 150)
#     sheet.format("A1:G1", {
#     "backgroundColor": {
#       "red": 0.7,
#       "green": 1.0,
#       "blue": 0.7
#     },
#     "wrapStrategy": 'WRAP',
#     "horizontalAlignment": "CENTER",
#     "verticalAlignment": "MIDDLE",
#     "textFormat": {
#       "bold": True
#     }
# })


def send_quarter_data_to_spreadsheet(df: DataFrame,
                                     engineer: str) -> Worksheet:
    '''
    Отсылает данные о баллах, заработанных в каждом квартале
    в таблицу "Премирование".
    '''
    try:
        sheet = gc.open(f'Премирование{dt.now().year}').worksheet(f'{engineer}')
    except gspread.exceptions.SpreadsheetNotFound:
        sh = gc.create(f'Премирование{dt.now().year}')
        sh.share('kroxxatt@gmail.com', perm_type='user', role='writer', notify=True)        #TODO с кем шерить доступ
        sheet = sh.add_worksheet(title=f'{engineer}', rows=200, cols=20)
    except gspread.exceptions.WorksheetNotFound:
        sh = gc.open(f'Премирование{dt.now().year}')
        sheet = sh.add_worksheet(title=f'{engineer}', rows=200, cols=20)
    
     #TODO сделать расчет для определения диапазона?

    sheet.update([df.columns.values.tolist()] + df.values.tolist(), range_name='J1:K200')

                                                                    


def main_func(engineers: list[str], df: DataFrame):
    '''
    Собирает данные из архива проектов, производит расчет баллов
    и отправляет полученные данные в новую таблицу.
    '''
    for engineer in engineers:
        print(engineer)
        engineer_projects = df.loc[df['Разработал'].str.contains(f'{engineer}')]
        engineer_projects["Баллы"] = engineer_projects.apply(count_points, axis=1)
        send_data_to_spreadsheet(engineer_projects, engineer)

        engineer_projects_filtered = engineer_projects[[
            'Шифр (ИСП)', 'Разработал', 'Дата начала проекта', 'Дата окончания проекта', 'Баллы'
        ]].loc[engineer_projects['Баллы'] != 'Необходимо заполнить данные для расчёта']

        if not engineer_projects_filtered.empty:
            print(engineer_projects_filtered)
            quarters = calculate_quarter(engineer_projects_filtered)
            send_quarter_data_to_spreadsheet(quarters, engineer)
        



if __name__ == "__main__":
    gc = gspread.service_account(filename='creds.json')

    try:
       worksheet = gc.open("Таблица проектов").worksheet(f'{dt.now().year}')
    except gspread.exceptions.SpreadsheetNotFound:
        pass                                                                    #TODO уведомление, что кто-то изменил название, или произошла смена таблицы
    except gspread.exceptions.WorksheetNotFound:    #таблица не найдена при смене года, создать листок      
        spreadsheet = gc.open("Таблица проектов")
        worksheet = spreadsheet.add_worksheet(f'{dt.now().year}')

    df = pd.DataFrame(worksheet.get_all_records())

    if not df.empty:
        list_of_engineers = get_list_of_engineers(df)
        try:
            salary_bonus = main_func(list_of_engineers, df)
        except gspread.exceptions.APIError as error:                   #TODO уведомление об ошибке
            raise TooManyRequestsApiError(error)
