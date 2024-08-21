from datetime import datetime as dt
from datetime import timedelta

import holidays
import pandas as pd
from pandas.core.frame import DataFrame

pd.options.mode.chained_assignment = None

# def set_project_complexity(row: DataFrame) -> int:
#     '''
#     Устанавливает сложность объекта.
#     Сложность расчитывается от 1 до 5.
#     '''
#     first_type = ['блок-контейнер', 'школа', 'больница', 'поликлин']
#     second_type = ['адм. здание', 'административное здание', 'медицинские учреждения']
#     third_type = ['производственное здание', 'пром. предприятие', 'выставка', 'музей', 'цгс']
#     fourth_type = ['цод','производственное здание', 'пром. предприятие']
#     fifth_type = ['цод','производственное здание', 'пром. предприятие']
#     for type in first_type:
#         if type in row['Тип объекта'].strip().lower():
#             return 1
#     for type in second_type:
#         if type in row['Тип объекта'].strip().lower():
#             return 2
#     for type in third_type:
#         if type in row['Тип объекта'].strip().lower():
#             if (int(row['Количество направлений']) <= 8) or (
#                 row['Тип объекта'].strip().lower() in ['выставка', 'музей']):
#                 return 3
#             else:
#                 return 4
#     for type in fourth_type:
#         if type in row['Тип объекта'].strip().lower():
#             if int(row['Площадь защищаемых помещений (м^2)']) < 500:
#                 return 4
#             else:
#                 return 5
    
#     if int(row['Количество направлений']) <= 8:
#         return 3
#     elif int(row['Площадь защищаемых помещений (м^2)']) < 500:
#         return 4
#     else:
#         return 5


def check_filled_projects(row: DataFrame) -> bool:
    '''
    Проверка на возможность подсчета баллов.
    Если какие-то характеристики отсутствуют, подсчет невозможен.
    '''
    characteristics = ['Наименование объекта', 'Шифр (ИСП)', 'Тип объекта',
                       'Дата начала проекта', 'Дата окончания проекта',
                       'Сложность']
    if 'блок-контейнер' in row['Тип объекта'].strip().lower():
        return True
    for char in characteristics:
        if row[f'{char}'] == '' or row[f'{char}'] == None:
            return False
    return True


def check_amount_directions(comp: int, amount: str) -> int:
    '''
    Расчитывает баллы за количество направлений в зависимости от сложности объекта.
    До 20 направлений баллы расчитываются по таблице complexity.
    Если направлений больше 20, баллы расчитываются по формуле:
    round((amount/(8.5-comp) + comp/2),1)
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


def check_square(comp: int, row: DataFrame) -> int:
    '''
    Расчитывает баллы в зависимости от площади
    защищаемого помещения, сложности объекта и проделанной работы.
    '''
    count = 0
    try:
        square = int(row['Площадь защищаемых помещений (м^2)'])
    except ValueError:
        square = 1

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
                    break
    for char in characteristics:
        if char == True:
            count += points
    return count


def check_sot_skud(row: DataFrame) -> int:
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


def check_cultural_heritage(row: DataFrame) -> int:
    '''
    Проверяет, считается ли объект культурным наследием.
    Если считается, то прибавляется 3 балла.
    '''
    if row['Объект культурного наследия'] == 'Да':
        return 3
    else:
        return 0


def check_net(row: DataFrame) -> int:
    '''
    Проверяет, закладываются ли сети в проект.
    '''
    if row['Сети'] == 'Есть':
        return 1.5
    else:
        return 0


def check_authors(authors: str) -> int:
    '''
    Считает количество проектировщиков, разрабатывающих проект.
    '''
    authors = authors.strip()
    if ',' in authors:
        authors = authors.split(',')
        return(len(authors))
    else:
        return 1


def count_non_working_days(start_date: dt.date, end_date: dt.date) -> int:
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


def check_spend_time(row: DataFrame, points: int) -> int:
    '''
    Проверка на соблюдение дэдлайнов проекта.
    Если проект выполнен в срок из расчета 1 балл = 5 рабочим дням,
    остаются те же баллы. Если дэдлайн был просрочен, полученные
    баллы умножаются на понижающий коэффициент.
    '''
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
