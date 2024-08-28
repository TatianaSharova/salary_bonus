import time
from datetime import datetime as dt

import gspread
import pandas as pd
from gspread.worksheet import Worksheet
from gspread_formatting import *
from pandas.core.frame import DataFrame
from pandas.core.series import Series

from exceptions import TooManyRequestsApiError
from worksheets import (connect_to_project_archive, connect_to_bonus_ws,
                        color_overdue_deadline, connect_to_settings_ws)

pd.options.mode.chained_assignment = None
from counting_points import (check_amount_directions, check_authors,
                             check_cultural_heritage, check_filled_projects,
                             check_net, check_sot_skud, check_spend_time,
                             check_square)
from quaterly_points import calculate_quarter

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
    # points += check_amount_directions(complexity, row['Другие АПТ (количество направлений)'])
    points += check_square(complexity, row)
    points += check_sot_skud(row)
    points += check_cultural_heritage(row)
    points += check_net(row)
    points = round(points/check_authors(row['Разработал']),1)
    points = check_spend_time(row, points, df)

    return points


def send_data_to_spreadsheet(df: DataFrame, engineer: str) -> Worksheet:
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
    

    # sheet.clear()

    
    eng_small = df[['Страна', 'Наименование объекта', 'Шифр (ИСП)', 'Разработал', 'Баллы',
                    'Дата начала проекта', 'Дата окончания проекта', 'Дедлайн']]
    
    sheet.update([eng_small.columns.values.tolist()] + eng_small.values.tolist())

    color_overdue_deadline(eng_small, sheet)
#     set_column_width(sheet, 'A', 100)
#     set_column_width(sheet, 'B', 400)
#     set_column_width(sheet, 'C', 200)
#     set_column_width(sheet, 'D', 150)
#     set_column_width(sheet, 'E', 150)
#     set_column_width(sheet, 'F', 150)
#     set_column_width(sheet, 'G', 150)
#     sheet.format("A1:H1", {
#     "backgroundColor": {
#       "red": 0.7,
#       "green": 1.0,
#       "blue": 0.7
#     },
#     "textFormat": {
#       "bold": True
#     }
# })
#     sheet.format('A1:K200', {
#     "wrapStrategy": 'WRAP',
#     "horizontalAlignment": "CENTER",
#     "verticalAlignment": "MIDDLE",
#     })



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
    sheet.format("J1:K1", {
        "backgroundColor": {
            "red": 0.8,
          "green": 0.9,
          "blue": 1
        },
        "textFormat": {
          "bold": True
        }
    })
    time.sleep(20)

                                                                    


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
        



if __name__ == "__main__":
    gc = gspread.service_account(filename='creds.json')

    while True:

        worksheet = connect_to_project_archive()

        df = pd.DataFrame(worksheet.get_all_records())

        if not df.empty:
            list_of_engineers = get_list_of_engineers(df)
            if list_of_engineers != []:
                try:
                    salary_bonus = main_func(list_of_engineers, df)
                except gspread.exceptions.APIError as error:                   #TODO уведомление об ошибке
                    raise TooManyRequestsApiError(error)
