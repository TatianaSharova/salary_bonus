import time
from datetime import datetime as dt

import gspread
import pandas as pd
from gspread.worksheet import Worksheet
from gspread_formatting import *
from pandas.core.frame import DataFrame
from pandas.core.series import Series
from gspread_formatting import get_ranges, get_conditional_format_rules, get_frozen_rows, get_data_validation_rules
from gspread_formatting import set_row_heights, set_column_widths, format_cell_ranges, set_frozen
from gspread_formatting import apply_formatting

gc = gspread.service_account(filename='creds.json')

def fill_the_new_worksheet(worksheet: Worksheet):

# Открываем Google Spreadsheet по названию
    spreadsheet = gc.open("Название таблицы")

# Открываем листы по названию
source_sheet = spreadsheet.worksheet("Название исходного листа")
destination_sheet = spreadsheet.worksheet("Название целевого листа")

# Считываем данные с исходного листа
data = source_sheet.get_all_values()

# Очищаем целевой лист
destination_sheet.clear()

# Вставляем данные на целевой лист
for i, row in enumerate(data):
    destination_sheet.insert_row(row, i + 1)

# Копируем форматирование
# Копируем размеры строк
set_row_heights(destination_sheet, get_ranges(source_sheet.row_heights()), get_ranges(destination_sheet.row_heights()))
# Копируем размеры столбцов
set_column_widths(destination_sheet, get_ranges(source_sheet.column_widths()), get_ranges(destination_sheet.column_widths()))
# Копируем замороженные строки
set_frozen(destination_sheet, rows=1)
# Копируем правила форматирования
format_cell_ranges(destination_sheet, get_ranges(source_sheet.formats()))
# Копируем правила условного форматирования
destination_sheet.conditional_format_rules = get_conditional_format_rules(source_sheet)
# Копируем правила проверки данных
destination_sheet.data_validation_rules = get_data_validation_rules(source_sheet)



def connect_to_project_archive():
    try:
       worksheet = gc.open("Таблица проектов").worksheet(f'{dt.now().year}')
    except gspread.exceptions.SpreadsheetNotFound:
        pass                                                                    #TODO уведомление, что кто-то изменил название, или произошла смена таблицы
    except gspread.exceptions.WorksheetNotFound:    #таблица не найдена при смене года, создать листок      
        spreadsheet = gc.open("Таблица проектов")
        worksheet = spreadsheet.add_worksheet(f'{dt.now().year}')
        fill_the_new_worksheet(worksheet)
