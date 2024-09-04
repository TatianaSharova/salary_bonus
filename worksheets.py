import os
import time
from datetime import datetime as dt

import gspread
from gspread.spreadsheet import Spreadsheet
from gspread.worksheet import Worksheet
from gspread_formatting import *
from pandas.core.frame import DataFrame

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
creds_path = os.path.join(BASE_DIR, 'creds.json')
env_path = os.path.join(BASE_DIR, '.env')
gc = gspread.service_account(filename=creds_path)


def get_column_letter(n: int) -> str:
    '''
    Преобразует номер столбца в буквенное обозначение.
    Например, 1 -> A, 27 -> AA.
    '''
    string = ""
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        string = chr(65 + remainder) + string
    return string


def create_new_ws_project_archive(spreadsheet: Spreadsheet) -> Worksheet:
    '''
    Создает новый лист для архива проектов
    с таким же форматированием, как у листа прошлого года.
    '''
    source_sheet = spreadsheet.worksheet(f'{dt.now().year - 1}')
    source_sheet_title = source_sheet.title

    destination_spreadsheet_id = spreadsheet.id
    source_sheet.copy_to(f'{destination_spreadsheet_id}')

    new_sheet = spreadsheet.worksheet(f'{source_sheet_title} (копия)')

    new_sheet.update_title(f'{dt.now().year}')
    
    total_rows = new_sheet.row_count
    total_cols = new_sheet.col_count

    last_column_letter = get_column_letter(total_cols)

    new_sheet.batch_clear([f'A2:{last_column_letter}{total_rows}'])
    return new_sheet



def connect_to_project_archive() -> Worksheet:
    '''
    Открывает лист с архивом проектов.
    При смене года создает новый лист.
    '''
    try:
       worksheet = gc.open('Таблица проектов').worksheet(f'{dt.now().year}')
    except gspread.exceptions.WorksheetNotFound:     
        spreadsheet = gc.open('Таблица проектов')
        worksheet = create_new_ws_project_archive(spreadsheet)
    
    return worksheet

def add_settings_ws(spreadsheet: Spreadsheet) -> Worksheet:
    '''Создает лист "Настройки" и форматирует его.'''
    sheet = spreadsheet.add_worksheet(title='Настройки', rows=100, cols=20)
    sheet.update([['Не учитывать']], 'A1')
    sheet.format('A1:C3', {
        'wrapStrategy': 'WRAP',
        'horizontalAlignment': 'CENTER',
        'verticalAlignment': 'MIDDLE'
    })
    sheet.format('A1', {
        'backgroundColor': {
            'red': 1,
            'green': 0.8,
            'blue': 0.8
        },
        'textFormat': {
            'fontSize': 12
            }
    })
    set_column_width(sheet, 'A', 300)

    return sheet


def connect_to_bonus_ws() -> Spreadsheet:
    '''
    Открывает таблицу "Премирование".
    При смене года создает новую таблицу.
    '''
    try:
        spreadsheet = gc.open(f'Премирование{dt.now().year}')
    except gspread.exceptions.SpreadsheetNotFound:
        spreadsheet = gc.create(f'Премирование{dt.now().year}')
        spreadsheet.share('kroxxatt@gmail.com', perm_type='user', role='writer', notify=True)        #TODO с кем шерить доступ
        worksheet = add_settings_ws(spreadsheet)

    return spreadsheet


def connect_to_settings_ws() -> Worksheet:
    '''Открывает лист "Настройки" из таблицы "Премирование".'''
    sheet = connect_to_bonus_ws()
    try:
        settings = sheet.worksheet(f'Настройки')
    except gspread.exceptions.WorksheetNotFound:
        settings = add_settings_ws(sheet)

    return settings


def color_overdue_deadline(df: DataFrame, sheet: Worksheet) -> Worksheet:
    '''Окрашивает ячейки с просроченным дедлайном.'''
    sheet.format('H2:H200', {
        'backgroundColor': {
            'red': 1,
            'green': 1,
            'blue': 1
        },
    })
    for index, row in df.iterrows():
        try:
            end_date = dt.strptime(row['Дата окончания проекта'], '%d.%m.%Y').date()
            deadline = dt.strptime(row['Дедлайн'], '%d.%m.%Y').date()
        except ValueError:
            continue

        if deadline < end_date:
            sheet.format(f'H{index + 2}', {
                'backgroundColor': {
                    'red': 1,
                    'green': 0.8,
                    'blue': 0.8
                    },
            })


def create_engineer_ws(spreadsheet: Spreadsheet, engineer: str) -> Worksheet:
    '''
    Создает для проектироващика лист и форматирует его.
    '''
    sheet = spreadsheet.add_worksheet(title=f'{engineer}', rows=200, cols=20)

    set_column_widths(sheet, [('A', 100), ('B', 400), ('C', 200), ('D:G', 150)])

    sheet.format('A1:H1', {
        'backgroundColor': {
        'red': 0.7,
        'green': 1.0,
        'blue': 0.7
        },
        'textFormat': {
            'bold': True
        }
    })
    sheet.format('A1:K200', {
        'wrapStrategy': 'WRAP',
        'horizontalAlignment': 'CENTER',
        'verticalAlignment': 'MIDDLE',
    })
    sheet.format('J1:K1', {
        'backgroundColor': {
            'red': 0.8,
            'green': 0.9,
            'blue': 1
        },
        'textFormat': {
          'bold': True
        }
    })
    set_frozen(sheet, rows=1)

    time.sleep(30)

    return sheet


def send_project_data_to_spreadsheet(df: DataFrame, engineer: str) -> Worksheet:
    '''
    Отправляет данные с баллами в таблицу "Премирование".
    '''
    spreadsheet = connect_to_bonus_ws()

    try:
        sheet = spreadsheet.worksheet(f'{engineer}')
    except gspread.exceptions.WorksheetNotFound:
        sheet = create_engineer_ws(spreadsheet, engineer)
    

    # sheet.clear()

    
    eng_small = df[['Страна', 'Наименование объекта', 'Шифр (ИСП)', 'Разработал', 'Баллы',
                    'Дата начала проекта', 'Дата окончания проекта', 'Дедлайн']]
    
    sheet.update([eng_small.columns.values.tolist()] + eng_small.values.tolist())

    color_overdue_deadline(eng_small, sheet)


def send_quarter_data_to_spreadsheet(df: DataFrame,
                                     engineer: str) -> Worksheet:
    '''
    Отсылает данные о баллах, заработанных в каждом квартале
    в таблицу "Премирование".
    '''
    spreadsheet = connect_to_bonus_ws()

    try:
        sheet = spreadsheet.worksheet(f'{engineer}')
    except gspread.exceptions.WorksheetNotFound:
        sheet = create_engineer_ws(spreadsheet, engineer)


    sheet.update([df.columns.values.tolist()] + df.values.tolist(), range_name='J1:K200')


def send_adj_data_to_spreadsheet(df: DataFrame,
                                 engineer: str) -> Worksheet:
    '''
    Отсылает данные о сделанных корректировках.
    '''
    spreadsheet = connect_to_bonus_ws()

    try:
        sheet = spreadsheet.worksheet(f'{engineer}')
    except gspread.exceptions.WorksheetNotFound:
        sheet = create_engineer_ws(spreadsheet, engineer)


    
    sheet.merge_cells('M1:Q1')
    sheet.update([['Корректировки']], 'M1')
    sheet.format('M1:Q200', {
        'wrapStrategy': 'WRAP',
        'horizontalAlignment': 'CENTER',
        'verticalAlignment': 'MIDDLE',
    })

    sheet.update([df.columns.values.tolist()] + df.values.tolist(), range_name='M2:Q200')
