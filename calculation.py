import gspread
from datetime import datetime as dt
import pandas as pd
from pandas.core.frame import DataFrame
pd.options.mode.chained_assignment = None

# lis = ['Наименование объекта', 'Шифр (ИСП)', 'Тип объекта', 'Тип защищаемых помещений',
#        'Количество направлений', 'Площадь защищаемых помещений (м^2)']

gc = gspread.service_account(filename='creds.json')

worksheet = gc.open("test_sal").worksheet(f'{dt.now().year}')

df = pd.DataFrame(worksheet.get_all_records())


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
    '''Проверка на возможность подсчета баллов.
    Если какие-то характеристики отсутствуют, подсчет невозможен.'''
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
    '''Расчитывает баллы в зависимости от площади
    защищаемого помещения, сложности объекта и проделанной работы.'''
    count = 0
    print(row['Шифр (ИСП)'])
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


def check_stm_skud(row):
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
    if row['Объект культурного наследия'] == 'Да':
        return 3
    else:
        return 0


def check_net(row):
    if row['Сети'] == 'Есть':
        return 1.5
    else:
        return 0


def check_authors(authors):
    authors = authors.strip()
    if ',' in authors:
        authors = authors.split(',')
        return(len(authors))
    else:
        return 1

def check_spend_time(row, points):
    '''Проверка на соблюдение дэдлайнов проекта.
     Если проект выполнен в срок из расчета 1 балл = 5 рабочим дням,
      остаются те же баллы. Если дэдлайн был просрочен, полученные
       баллы умножаются на понижающий коэффициент. '''
    coefficient = 1                                                 #TODO определить понижающий коэффициент за просрочку дэдлайна
    days_deadline = points*7

    start_date = dt.strptime(row['Дата начала проекта'], "%d.%m.%Y").date()
    end_date = dt.strptime(row['Дата окончания проекта'], "%d.%m.%Y").date()

    spend_time_for_project = (end_date - start_date).days
    if spend_time_for_project <= days_deadline:                        #TODO не считать гос выходные
        return points
    else:
        return points*coefficient





def count_points(row):
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
    points += check_stm_skud(row)
    points += check_cultural_heritage(row)
    points += check_net(row)
    points = round(points/check_authors(row['Разработал']),1)
    points = check_spend_time(row, points)
    return points


def send_data_to_spreadsheet(df: DataFrame, engineer: str):
    try:
        sheet = gc.open("Премирование").worksheet(f'{engineer}')
    except gspread.exceptions.SpreadsheetNotFound:
        pass                                                                 #TODO создание таблицы, если она не находится
    except gspread.exceptions.WorksheetNotFound:
        sh = gc.open("Премирование")
        sheet = sh.add_worksheet(title=f'{engineer}', rows=200, cols=20)
    
    sheet.clear()
    
    eng_small = df[['Страна', 'Наименование объекта', 'Шифр (ИСП)', 'Шифр (сторонней организации)', 'Разработал', 'Баллы']]
    print(eng_small)
    
    sheet.update([eng_small.columns.values.tolist()] + eng_small.values.tolist())
    sheet.format("A1:F1", {
    "backgroundColor": {
      "red": 0.7,
      "green": 1.0,
      "blue": 0.7
    },
    "wrapStrategy": 'WRAP',
    "horizontalAlignment": "CENTER",
    "textFormat": {
      "bold": True
    }
})
                                                                       





def func(engineers: list, df: DataFrame):
    for engineer in engineers:
        print(engineer)
        engineer_projects = df[df['Разработал'].str.contains(f'{engineer}')]
        engineer_projects["Баллы"] = engineer_projects.apply(count_points, axis=1)
        send_data_to_spreadsheet(engineer_projects, engineer)
        #print(engineer_projects)



# Сортировка по имени инженера


if __name__ == "__main__":
    a = get_list_of_engineers(df)

    b = func(a, df)


