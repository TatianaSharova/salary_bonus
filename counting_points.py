from datetime import datetime as dt
from datetime import timedelta
from typing import Union

import holidays
import pandas as pd
from pandas.core.frame import DataFrame
from pandas.core.series import Series

pd.options.mode.chained_assignment = None


def check_filled_projects(row: Series) -> bool:
    '''
    Проверка на возможность подсчета баллов.
    Если какие-то характеристики отсутствуют, подсчет невозможен.
    '''
    characteristics = ['Наименование объекта', 'Шифр (ИСП)',
                       'Тип объекта', 'Дата начала проекта']
    if 'блок-контейнер' in row['Тип объекта'].strip().lower():
        return True
    for char in characteristics:
        if row[f'{char}'] == '' or row[f'{char}'] is None:
            return False
    return True


def check_amount_directions(comp: int, amount: str) -> int:
    '''
    Расчитывает баллы за количество направлений
    в зависимости от сложности объекта.
    До 20 направлений баллы расчитываются по таблице complexity.
    Если направлений больше 20, баллы расчитываются по формуле:
    round((amount/(8.5-comp) + comp/2),1)
    '''
    complexity = {
        1: [(1, 4), (1.5, 6), (2, 8), (2.5, 12), (3, 20)],
        2: [(1, 2), (1.5, 4), (2, 8), (3, 12), (3.5, 20)],
        3: [(1.5, 2), (2, 4), (2.5, 8), (3.5, 12), (4, 20)],
        4: [(2, 2), (2.5, 4), (3, 8), (4, 12), (5, 20)],
        5: [(4, 6), (5, 8), (6, 12), (8, 20)]
    }
    try:
        amount = int(amount)
    except ValueError:
        return 0

    for i in range(1, 6):
        if comp == i:
            for point_amount in complexity[i]:
                if amount <= point_amount[1]:
                    return point_amount[0]
            return round((amount/(8.5-comp) + comp/2), 1)
    return None


def define_square(square: str) -> float:
    '''
    Подготавливает введенные данные площади для дальнейших вычислений.
    '''
    try:
        square = float(square)
    except ValueError:
        if ',' in square:
            numbers = square.split(',')
            square = float(numbers[0])
        else:
            square = 1.0
    return square


def check_square(comp: int, row: Series) -> int:
    '''
    Расчитывает баллы в зависимости от площади
    защищаемого помещения, сложности объекта и проделанной работы.
    '''
    count = 0
    try:
        square = float(row['Площадь защищаемых помещений (м^2)'])
    except ValueError:
        square = define_square(row['Площадь защищаемых помещений (м^2)'])

    ps = row['ПС'] == 'Есть'
    os = row['ОС'] == 'Есть'
    soue = row['СОУЭ'] == 'Есть'
    asv = row['Автоматизация систем вентиляции'] == 'Есть'
    characteristics = [ps, os, soue, asv]

    complexity = {
        1: [(1, 400), (1.5, 1000), (2, 3000), (2.5, 10000), (3, 100000)],
        2: [(2, 400), (2.5, 1000), (3, 3000), (3.5, 10000), (5, 100000)],
        3: [(2, 400), (3, 1000), (3.5, 3000), (4, 10000), (8, 100000)],
        4: [(3, 400), (4, 1000), (4.5, 3000), (5, 10000), (10, 100000)],
        5: [(4, 400), (4.5, 1000), (5, 3000), (5.5, 10000), (12, 100000)]
    }

    for i in range(1, 6):
        if comp == i:
            for point_square in complexity[i]:
                if square <= point_square[1]:
                    points = point_square[0]
                    break
    for char in characteristics:
        if char is True:
            count += points
    return count


def check_sot_skud(row: Series) -> int:
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
            elif char <= 20:
                points += 1.5
            else:
                points += 2
    return points


def check_cultural_heritage(row: Series) -> int:
    '''
    Проверяет, считается ли объект культурным наследием.
    Если считается, то прибавляется 3 балла.
    '''
    if row['Объект культурного наследия'] == 'Да':
        return 3
    return 0


def check_net(row: Series) -> int:
    '''
    Проверяет, закладываются ли сети в проект.
    '''
    if row['Сети'] == 'Есть':
        return 1.5
    return 0


def check_authors(authors: str) -> int:
    '''
    Считает количество проектировщиков, разрабатывающих проект.
    '''
    authors = authors.strip()
    if ',' in authors:
        authors = authors.split(',')
        return len(authors)
    return 1


def calculate_deadline(start_date: dt.date, work_days: int,
                       row: Series) -> dt.date:
    '''Расчитывает дату дедлайна, исходя из
    даты начала и количества рабочих дней.'''
    current_date = start_date
    days_added = 0
    ru_holidays = holidays.RU()

    while days_added < work_days:
        if current_date.weekday() < 5 and current_date not in ru_holidays:
            days_added += 1
        current_date += timedelta(days=1)

    try:
        plus_days = int(row['Продление дедлайна'])
        current_date += timedelta(days=plus_days)
    except ValueError:
        plus_days = 0

    return current_date - timedelta(days=1)


def check_spend_time(row: Series, points: int,
                     df: DataFrame, amount: int) -> Union[float, str]:
    '''
    Проверка на соблюдение дедлайнов проекта.
    Если проект выполнен в срок из расчета 1 балл = 5 рабочим дням,
    остаются те же баллы. Если дедлайн был просрочен, полученные
    баллы умножаются на понижающий коэффициент.
    '''
    coefficient = 0.9
    if 'блок-контейнер' in row['Тип объекта'].strip().lower():
        days_deadline = amount + 4
    else:
        days_deadline = points*5

    try:
        start_date = dt.strptime(row['Дата начала проекта'], "%d.%m.%Y").date()
    except ValueError:
        return 'Некорректно введены даты.'

    deadline = calculate_deadline(start_date, days_deadline, row)
    deadline_str = deadline.strftime("%d.%m.%Y")
    df.loc[df['Шифр (ИСП)'] == row['Шифр (ИСП)'], 'Дедлайн'] = deadline_str

    try:
        end_date = dt.strptime(
            row['Дата окончания проекта'], "%d.%m.%Y"
        ).date()
    except ValueError:
        end_date = None
        if row['Дата окончания проекта'] == '':
            return f'{points} - предварительные баллы. Проект ещё не сдан.'
        return 'Некорректно введены даты.'

    if end_date:
        if deadline < end_date:
            points = points*coefficient

    return points


def count_deadline_for_blocks(row: Series, df: DataFrame, points):
    '''Отсортировывает блок-контейнеры по группам,
    чтобы посчитать для них общий дедлайн.
    Для одиночных контейнеров дедлайн считается, как для обычных проектов.
    '''
    name = row['Наименование объекта']
    start = row['Дата начала проекта']
    end = row['Дата окончания проекта']

    filtered_df = df[
        (df['Наименование объекта'] == name) &
        (df['Дата начала проекта'] == start) &
        (df['Дата окончания проекта'] == end)
    ]
    num_rows = filtered_df.shape[0]

    if num_rows == 1:
        row['Тип объекта'] = 'единственный контейн-р'
        return check_spend_time(row, points, df, amount=num_rows)
    if num_rows > 1:
        return check_spend_time(row, points, df, amount=num_rows)
    return None


def count_block(row: Series, blocks: list,
                points: int, df: DataFrame) -> float:
    '''
    Расчитывает баллы для блок-контейнеров.
    За первый разработанный объект - 1 балл,
    за остальные - 0.5 балла.
    '''
    name = row['Наименование объекта']

    points = count_deadline_for_blocks(row, df, points)

    if name in blocks:
        return 0.5*points
    blocks.append(name)
    return points


def count_points(
        row: Series, df: DataFrame, blocks: list
) -> Union[int, str]:
    '''
    Считает и возвращает сумму полученных баллов.
    Если подсчет произвести невозможно, то возвращает
    строку-предупреждение об этом.
    '''
    points = 0
    filled_project = check_filled_projects(row)
    if not filled_project:
        return 'Необходимо заполнить данные для расчёта'
    if 'да' in row['Является корректировкой'].strip().lower():
        return count_adjusting_points(row, df, blocks)

    complexity = int(row['Сложность для расчета'])

    points += check_amount_directions(complexity,
                                      row['Количество направлений'])
    points += check_square(complexity, row)
    points += check_sot_skud(row)
    points += check_cultural_heritage(row)
    points += check_net(row)
    points = round(points/check_authors(row['Разработал']), 1)

    if 'блок-контейнер' in row['Тип объекта'].strip().lower():
        return count_block(row, blocks, points, df)

    return check_spend_time(row, points, df, amount=0)


def count_adjusting_points(row: Series, df: DataFrame, blocks: list) -> float:
    '''
    Расчитывает баллы за корректировки:
    30 процентов от баллов за готовый проект.
    '''
    points = 0

    complexity = int(row['Сложность для расчета'])

    points += check_amount_directions(complexity,
                                      row['Количество направлений'])
    points += check_square(complexity, row)
    points += check_sot_skud(row)
    points += check_cultural_heritage(row)
    points += check_net(row)
    points = round(points/check_authors(row['Разработал']), 1)
    points = check_spend_time(row, points, df, amount=0)

    if isinstance(points, str):
        return points

    return points*0.3
