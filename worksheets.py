import os
import time
from datetime import datetime as dt

import gspread
from dotenv import load_dotenv
from gspread.spreadsheet import Spreadsheet
from gspread.worksheet import Worksheet
from gspread_formatting import *
from pandas.core.frame import DataFrame

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
creds_path = os.path.join(BASE_DIR, 'creds.json')
env_path = os.path.join(BASE_DIR, '.env')
load_dotenv(dotenv_path=env_path)

EMAIL = os.getenv('EMAIL')
EMAIL2 = os.getenv('EMAIL2')
EMAIL3 = os.getenv('EMAIL3')
EMAIL4 = os.getenv('EMAIL4')
ENDPOINT = os.getenv('ENDPOINT')

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
    sheet.format('A1:T40', {
        'wrapStrategy': 'WRAP',
        'horizontalAlignment': 'CENTER',
        'verticalAlignment': 'MIDDLE'
    })
    sheet.format(['A1', 'C1:D1', 'F1:R1'], {
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
        spreadsheet.share(EMAIL, perm_type='user', role='writer', notify=True)
        spreadsheet.share(EMAIL2, perm_type='user', role='writer', notify=True)
        spreadsheet.share(EMAIL3, perm_type='user', role='writer', notify=True)
        spreadsheet.share(EMAIL4, perm_type='user', role='writer', notify=True)
        worksheet = add_settings_ws(spreadsheet)
        sheet1 = spreadsheet.worksheet('Sheet1')
        spreadsheet.del_worksheet(sheet1)

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


def color_comp_correction(df: DataFrame, sheet: Worksheet) -> Worksheet:
    '''Окрашивает ячейки с учтенной коррекцией сложности.'''
    sheet.format('J2:J200', {
        'backgroundColor': {
            'red': 1,
            'green': 1,
            'blue': 1
        },
    })
    if 'Корректировка сложности' in df.columns:
        for index, row in df.iterrows():
            if (row['Корректировка сложности']
                and isinstance(row['Корректировка сложности'], str)
                and row['Корректировка сложности'].isdigit()):
                sheet.format(f'J{index + 2}', {
                    'backgroundColor': {
                        'red': 1,
                        'green': 1,
                        'blue': 0.8
                        },
                })


def create_engineer_ws(spreadsheet: Spreadsheet, engineer: str) -> Worksheet:
    '''
    Создает для проектироващика лист и форматирует его.
    '''
    sheet = spreadsheet.add_worksheet(title=f'{engineer}', rows=200, cols=20)

    set_column_widths(sheet, [('A', 100), ('B', 400), ('C', 200), ('D:G', 150),
                              ('I:J', 150)])
    sheet.update([['Корректировка сложности']], 'J1')
    sheet.update([['Отпуск/отгул/не работал(а) (в часах)']], 'Q1')
    sheet.format('A1:J1', {
        'backgroundColor': {
        'red': 0.7,
        'green': 1.0,
        'blue': 0.7
        },
        'textFormat': {
            'bold': True
        }
    })
    sheet.format('A1:T200', {
        'wrapStrategy': 'WRAP',
        'horizontalAlignment': 'CENTER',
        'verticalAlignment': 'MIDDLE',
    })
    sheet.format('L1:Q1', {
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


def connect_to_engineer_ws(engineer: str) -> Worksheet:
    '''Открывает лист проектировщика.'''
    spreadsheet = connect_to_bonus_ws()

    try:
        sheet = spreadsheet.worksheet(f'{engineer}')
    except gspread.exceptions.WorksheetNotFound:
        return None
    
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


    eng_small = df[['Страна', 'Наименование объекта', 'Шифр (ИСП)', 'Разработал', 'Баллы',
                    'Дата начала проекта', 'Дата окончания проекта', 'Дедлайн',
                     'Автоматически определенная сложность']]
    
    sheet.update([eng_small.columns.values.tolist()] + eng_small.values.tolist())

    color_overdue_deadline(eng_small, sheet)
    color_comp_correction(df, sheet)


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


    sheet.update([df.columns.values.tolist()] + df.values.tolist(), range_name='L1:N200')


def send_results_data_ws(df: DataFrame) -> Worksheet:
    '''Отправляет данные о средних баллах на лист "Настройки".'''
    worksheet = connect_to_settings_ws()

    worksheet.update([df.columns.values.tolist()] + df.values.tolist(), range_name='C1:D10')

    return worksheet


def send_bonus_data_ws(engineer: str, df: DataFrame) -> Worksheet:
    '''Отправляет данные о выполнении плана на лист проектировщика.'''
    worksheet = connect_to_engineer_ws(engineer)

    worksheet.update([df.columns.values.tolist()] + df.values.tolist(), range_name='N1:P10')

    return worksheet


def connect_to_attendance_sheet(month: str) -> Worksheet:
    '''Подключается к табелю посещаемости офиса.'''
    try:
        spreadsheet = gc.open_by_url(ENDPOINT)
    except gspread.exceptions.SpreadsheetNotFound:
        return None
    
    try:
        worksheet = spreadsheet.worksheet(month)
    except gspread.exceptions.WorksheetNotFound:
        try:
            month = month + ' '
            worksheet = spreadsheet.worksheet(month)
        except gspread.exceptions.WorksheetNotFound:
            return None
    
    return worksheet


def send_non_working_hours_ws(engineer: str, df: DataFrame) -> Worksheet:
    '''Отправляет данные о нерабочих часах на лист проектировщика.'''
    worksheet = connect_to_engineer_ws(engineer)

    non_working_hours = df['Нерабочие часы'].values.tolist()

    non_working_hours = [None if x == 0 else x for x in non_working_hours]

    worksheet.update([[cell] for cell in non_working_hours],  range_name='Q2:Q5')

    return worksheet


def send_hours_data_ws(df: DataFrame) -> Worksheet:
    '''Отправляет данные о рабочих часах на лист настроек.'''
    worksheet = connect_to_settings_ws()

    worksheet.update([df.columns.values.tolist()] + df.values.tolist(), range_name='F1:R30')

    return worksheet

