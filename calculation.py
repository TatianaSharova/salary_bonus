import gspread
from datetime import datetime as dt
import pandas as pd
from pandas.core.frame import DataFrame
pd.options.mode.chained_assignment = None

lis = ['Наименование объекта', 'Шифр (ИСП)', 'Тип объекта', 'Тип защищаемых помещений',
       'Количество направлений', 'Площадь защищаемых помещений (м^2)']

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
       'Количество направлений', 'Площадь защищаемых помещений (м^2)']
    if 'блок-контейнер' in row['Тип объекта'].strip().lower():                                #TODO надо ли это?
        return True
    for property in characteristics:
        if row[f'{property}'] == '':
            return False
    return True


def check_amount_directions(comp, row):
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
    amount = int(row['Количество направлений'])

    for i in range(1,6):
        if comp == i:
            for point_amount in complexity[i]:
                if amount <= point_amount[1]:
                    return point_amount[0]
            return round((amount/(8.5-comp) + comp/2),1)                              #TODO расчет, если направлений больше


def check_square(row):
    square = int(row['Площадь защищаемых помещений (м^2)'])




def count_points(row):
    points = 0
    filled_project = check_filled_projects(row)
    if not filled_project:
        return 'Необходимо заполнить данные для расчёта'
    
    complexity = set_project_complexity(row)
    points += check_amount_directions(complexity, row)
    points += check_square(row)

    

        

def func(engineers: list, df: DataFrame):
    for engineer in engineers:
        engineer_projects = df[df['Разработал'].str.match(f'{engineer}')]
        engineer_projects["Баллы"] = engineer_projects.apply(count_points, axis=1)
        print(engineer_projects)







# Сортировка по имени инженера
Kulikov = df[df['Разработал'].str.match('Куликов')]

if __name__ == "__main__":
    a = get_list_of_engineers(df)

    b = func(a, df)


