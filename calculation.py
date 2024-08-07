import gspread
from datetime import datetime as dt
import pandas as pd
from pandas.core.frame import DataFrame

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


def check_filled_projects(row):
    points = 0
    lis = ['Наименование объекта', 'Шифр (ИСП)', 'Тип объекта',
       'Количество направлений', 'Площадь защищаемых помещений (м^2)']
    for char in lis:
        if row[f'{char}'] == '':
            return 0
    return 1
    

        

def func(engineers: list, df: DataFrame):
    for engineer in engineers:
        engineer_projects = df[df['Разработал'].str.match(f'{engineer}')]
        engineer_projects["points"] = df.apply(check_filled_projects, axis=1)
        print(engineer_projects)







# Сортировка по имени инженера
Kulikov = df[df['Разработал'].str.match('Куликов')]

if __name__ == "__main__":
    a = get_list_of_engineers(df)

    b = func(a, df)

