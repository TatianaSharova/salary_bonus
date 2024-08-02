import gspread
from datetime import datetime as dt
import pandas as pd
from pandas.core.frame import DataFrame

gc = gspread.service_account(filename='creds.json')

worksheet = gc.open("test_sal").worksheet(f'{dt.now().year}')

df = pd.DataFrame(worksheet.get_all_records())

engineers = set(df['Разработал'])

def get_list_of_engineers(df: DataFrame):
    '''Возвращает список инженеров.'''
    engineers = set(df['Разработал'])
    groups_of_engineers = set()
    s = set()

    if '' in engineers:
        engineers.remove('')

    for eng in engineers:
        if ',' in eng:
            groups_of_engineers.add(eng)

    for group in groups_of_engineers:
        s.update(group.split(','))
        engineers.remove(group)

    engineers.union(s)
    return engineers


# Сортировка по имени инженера
Kulikov = df[df['Разработал'].str.match('Куликов')]

if __name__ == "__main__":
    print(get_list_of_engineers(df))

