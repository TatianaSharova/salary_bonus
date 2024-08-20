import gspread
from datetime import datetime as dt
from datetime import timedelta
import pandas as pd
from pandas.core.frame import DataFrame
import holidays
from gspread_formatting import *
import time
from exceptions import TooManyRequestsApiError
pd.options.mode.chained_assignment = None


def get_list_of_engineers(df: DataFrame) -> list:
    '''Возвращает список инженеров.'''
    engineers = set(df['Разработал'])
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
    return list(engineers)


def set_project_complexity(row):
    '''
    Устанавливает сложность объекта.
    Сложность расчитывается от 1 до 5.
    '''
    first_type = ['блок-контейнер', 'школа', 'больница', 'поликлин']
    second_type = ['адм. здание', 'административное здание', 'медицинские учреждения']
    third_type = ['производственное здание', 'пром. предприятие', 'выставка', 'музей', 'цгс']
    fourth_type = ['цод','производственное здание', 'пром. предприятие']
    fifth_type = ['цод','производственное здание', 'пром. предприятие']
    for type in first_type:
        if type in row['Тип объекта'].strip().lower():
            return 1
    for type in second_type:
        if type in row['Тип объекта'].strip().lower():
            return 2
    for type in third_type:
        if type in row['Тип объекта'].strip().lower():
            if (int(row['Количество направлений']) <= 8) or (
                row['Тип объекта'].strip().lower() in ['выставка', 'музей']):
                return 3
            else:
                return 4
    for type in fourth_type:
        if type in row['Тип объекта'].strip().lower():
            if int(row['Площадь защищаемых помещений (м^2)']) < 500:
                return 4
            else:
                return 5
    
    if int(row['Количество направлений']) <= 8:
        return 3
    elif int(row['Площадь защищаемых помещений (м^2)']) < 500:
        return 4
    else:
        return 5


def check_filled_projects(row) -> bool:
    '''
    Проверка на возможность подсчета баллов.
    Если какие-то характеристики отсутствуют, подсчет невозможен.
    '''
    characteristics = ['Наименование объекта', 'Шифр (ИСП)', 'Тип объекта',
       'Количество направлений', 'Площадь защищаемых помещений (м^2)',
       'Дата начала проекта', 'Дата окончания проекта']
    if 'блок-контейнер' in row['Тип объекта'].strip().lower():
        return True
    for char in characteristics:
        if row[f'{char}'] == '' or row[f'{char}'] == None:
            return False
    return True


def check_amount_directions(comp, amount):
    '''
    Расчитывает баллы за количество направлений
    в зависимости от сложности объекта.
    '''
    complexity = {
        1 : [(1,4),(1.5,6),(2,8),(2.5,12),(3,20)],
        2 : [(1,2),(1.5,4),(2,8),(3,12),(3.5,20)],
        3 : [(1.5,2),(2,4),(2.5,8),(3.5,12),(4,20)],
        4 : [(2,2),(2.5,4),(3,8),(4,12),(5,20)],
        5 : [(4,6),(5,8),(6,12),(8,20)]
    }
    try:
        amount = int(amount)
    except ValueError:
        return 0

    for i in range(1,6):
        if comp == i:
            for point_amount in complexity[i]:
                if amount <= point_amount[1]:
                    return point_amount[0]
            return round((amount/(8.5-comp) + comp/2),1)


def check_square(comp, row):
    '''
    Расчитывает баллы в зависимости от площади
    защищаемого помещения, сложности объекта и проделанной работы.
    '''
    count = 0
    square = int(row['Площадь защищаемых помещений (м^2)'])

    ps = row['ПС'] == 'Есть'
    os = row['ОС'] == 'Есть'
    soue = row['СОУЭ'] == 'Есть'
    asv = row['Автоматизация систем вентиляции'] == 'Есть'
    characteristics = [ps,os,soue,asv]

    complexity = {
        1 : [(0,10000)],
        2 : [(2,400),(2.5,1000),(3,3000),(3.5,10000),(5,100000)],
        3 : [(2,400),(3,1000),(3.5,3000),(4,10000),(8,100000)],
        4 : [(3,400),(4,1000),(4.5,3000),(5,10000),(10,100000)],
        5 : [(4,400),(4.5,1000),(5,3000),(5.5,10000),(12,100000)]
    }

    for i in range(1,6):
        if comp == i:
            for point_square in complexity[i]:
                if square <= point_square[1]:
                    points = point_square[0]
                    break                                                   #TODO
    for char in characteristics:
        if char == True:
            count += points
    return count


def check_sot_skud(row):
    '''Расчитывает баллы в зависимости от СОТ, СКУД.'''
    points = 0
    try:
        stm = int(row['СОТ (количество камер)'])
    except ValueError:
        stm = 0
    try:
        skud = int(row['СКУД (количество точек доступа)'])
    except ValueError:
        skud = 0
    characteristics = [stm, skud]
    
    for char in characteristics:
        if char > 0:
            if char <= 10:
                points += 1
            elif char <= 20 :
                points += 1.5
            else:
                points += 2
    return points


def check_cultural_heritage(row):
    '''Проеряет, считается ли объект культурным наследием.'''
    if row['Объект культурного наследия'] == 'Да':
        return 3
    else:
        return 0


def check_net(row):
    '''Проверяет, закладываются ли
    сети в проет.'''
    if row['Сети'] == 'Есть':
        return 1.5
    else:
        return 0


def check_authors(authors):
    '''Считает количество проектировщиков,
    разрабатывающих проект.'''
    authors = authors.strip()
    if ',' in authors:
        authors = authors.split(',')
        return(len(authors))
    else:
        return 1


def count_non_working_days(start_date, end_date):
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


def check_spend_time(row, points):
    '''Проверка на соблюдение дэдлайнов проекта.
    Если проект выполнен в срок из расчета 1 балл = 5 рабочим дням,
    остаются те же баллы. Если дэдлайн был просрочен, полученные
    баллы умножаются на понижающий коэффициент. '''
    coefficient = 1                                                 #TODO определить понижающий коэффициент за просрочку дэдлайна
    days_deadline = points*5

    start_date = dt.strptime(row['Дата начала проекта'], "%d.%m.%Y").date()         #TODO обновление пакета holidays на сервере
    end_date = dt.strptime(row['Дата окончания проекта'], "%d.%m.%Y").date()

    spend_time_including_holidays = (end_date - start_date).days + 1
    holidays_amount = count_non_working_days(start_date, end_date)
    spend_working_time = spend_time_including_holidays - holidays_amount
    
    if spend_working_time <= days_deadline:
        return points
    else:
        return points*coefficient



def count_points(row):
    '''
    Считает сумму полученных баллов.
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
    
    # sheet.clear()
    
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
def calculate_quarter_points(row):
    '''
    
    '''
    row['Дата начала проекта'] = pd.to_datetime(row['Дата начала проекта'],
                                                   dayfirst=True, format='%d.%m.%Y')
    row['Дата окончания проекта'] = pd.to_datetime(row['Дата окончания проекта'],
                                                          dayfirst=True, format='%d.%m.%Y')

    try:
        quarters = pd.period_range(start=row['Дата начала проекта'], end=row['Дата окончания проекта'], freq='Q')
    except ValueError:
        month = row['Шифр (ИСП)'][5:7]
        year = row['Шифр (ИСП)'][:4]
        row['Дата начала проекта'] = pd.to_datetime(f'01.{month}.{year}',
                                                   dayfirst=True, format='%d.%m.%Y')
        row['Дата окончания проекта'] = pd.to_datetime(f'28.{month}.{year}',
                                                       dayfirst=True, format='%d.%m.%Y')
        quarters = pd.period_range(start=row['Дата начала проекта'], end=row['Дата окончания проекта'], freq='Q')

    quarter_points = []

    for quarter in quarters:
        quarter_start = max(pd.Timestamp(quarter.start_time), row['Дата начала проекта'])
        # print(quarter_start)
        quarter_end = min(pd.Timestamp(quarter.end_time), row['Дата окончания проекта'])
        # print(quarter_end)
        days_in_quarter = (quarter_end - quarter_start).days + 1
        total_days = (row['Дата окончания проекта'] - row['Дата начала проекта']).days + 1
        # print(days_in_quarter, total_days)
        proportion = days_in_quarter / total_days
        points = round(row['Баллы'] * proportion, 2)
        # print(proportion, points)
        quarter_points.append({
            'Шифр_ИСП': row['Шифр (ИСП)'],
            'Квартал': quarter,
            'Баллы': points
        })
    # print(quarter_points)
    return quarter_points

def calculate_quarter(df: DataFrame) -> DataFrame:
    '''
    
    '''
    quarterly_points = df.apply(calculate_quarter_points, axis=1)
    # print(quarterly_points)

    quarterly_points = [item for sublist in quarterly_points for item in sublist]
    # print(quarterly_points)

    quarterly_df = pd.DataFrame(quarterly_points)
    # print(quarterly_df)

    result = quarterly_df.groupby('Квартал')['Баллы'].sum().reset_index()
    result['Квартал'] = result['Квартал'].apply(lambda x: f"{x.quarter}-{x.year}")

    print(result)
    return result


def send_quarter_data_to_spreadsheet(df, engineer):
    '''
    Отсылает данные о баллах, заработанных в каждом квартале
    в таблицу "Премирование".
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
    
    
    # num_rows = len(df) + 1
    # num_cols = len(df.columns)
    # print(num_rows,num_cols)

    # start_col = 'J'
    # start_row = 1

    # end_col = chr(ord(start_col) + num_cols - 1)
    # end_row = start_row + num_rows - 1

    # range_name = f'{start_col}{start_row}:{end_col}{end_row}'             #TODO сделать расчет для определения диапазона
    # print(range_name)

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
       worksheet = gc.open("Копия Таблица проектов").worksheet(f'{dt.now().year}')
    except gspread.exceptions.SpreadsheetNotFound:
        pass                                                                    #TODO уведомление, что кто-то изменил название, или произошла смена таблицы
    except gspread.exceptions.WorksheetNotFound:    #таблица не найдена при смене года, создать листок      
        spreadsheet = gc.open("Копия Таблица проектов")
        worksheet = spreadsheet.add_worksheet(f'{dt.now().year}')

    df = pd.DataFrame(worksheet.get_all_records())

    if not df.empty:
        list_of_engineers = get_list_of_engineers(df)
        try:
            salary_bonus = main_func(list_of_engineers, df)
        except gspread.exceptions.APIError as error:                   #TODO уведомление об ошибке
            raise TooManyRequestsApiError(error)
