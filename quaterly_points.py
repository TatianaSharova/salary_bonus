from datetime import datetime as dt

import pandas as pd
from pandas.core.frame import DataFrame
from pandas.core.indexes.period import PeriodIndex
from pandas.core.series import Series

pd.options.mode.chained_assignment = None


def find_period(row: DataFrame) -> PeriodIndex:
    '''
    Расчитывает период от даты начала и конца.
    Дату начала берет из шифра ИСП. Дата окончания считается концом месяца.
    '''
    try:
        quarters = pd.period_range(start=row['Дата начала проекта'],
                                   end=row['Дата окончания проекта'], freq='Q')
    except ValueError:
        month = row['Шифр (ИСП)'][5:7]
        year = row['Шифр (ИСП)'][:4]
        row['Дата начала проекта'] = pd.to_datetime(f'01.{month}.{year}',
                                                    dayfirst=True,
                                                    format='%d.%m.%Y')
        row['Дата окончания проекта'] = pd.to_datetime(f'28.{month}.{year}',
                                                       dayfirst=True,
                                                       format='%d.%m.%Y')
        quarters = pd.period_range(start=row['Дата начала проекта'],
                                   end=row['Дата окончания проекта'], freq='Q')

    return quarters


def calculate_quarter_points(row: Series, colomn: str) -> list[dict]:
    '''
    Производит распределение баллов по кварталам.
    Если проект был выполнен в течение одного квартала,
    все баллы пойдут в этот квартал.

    Если проект был выполнен в течение нескольких кварталов,
    то баллы будут распределены по кварталам пропорционально времени,
    потраченному на проект в каждом квартале.
    '''
    row['Дата начала проекта'] = pd.to_datetime(
        row['Дата начала проекта'], dayfirst=True, format='%d.%m.%Y')
    row['Дата окончания проекта'] = pd.to_datetime(
        row['Дата окончания проекта'], dayfirst=True, format='%d.%m.%Y')

    if row['Дата начала проекта'] > row['Дата окончания проекта']:
        row['Дата начала проекта'], row['Дата окончания проекта'] = row['Дата окончания проекта'], row['Дата начала проекта']

    try:
        quarters = pd.period_range(start=row['Дата начала проекта'],
                                   end=row['Дата окончания проекта'], freq='Q')
    except ValueError:
        quarters = find_period(row)

    quarter_points = []

    for quarter in quarters:
        quarter_start = max(
            pd.Timestamp(quarter.start_time), row['Дата начала проекта']
        )
        quarter_end = min(
            pd.Timestamp(quarter.end_time), row['Дата окончания проекта']
        )
        days_in_quarter = (quarter_end - quarter_start).days + 1

        total_days = (row[
            'Дата окончания проекта'] - row['Дата начала проекта']).days + 1

        proportion = days_in_quarter / total_days
        points = round(row[colomn] * proportion, 2)
        quarter_points.append({
            'Шифр_ИСП': row['Шифр (ИСП)'],
            'Квартал': quarter,
            colomn: points
        })
    return quarter_points


def calculate_quarter(df: DataFrame, colomn: str) -> DataFrame:
    '''
    Создает таблицу с кварталами и заработанными баллами в каждом квартале.
    '''
    quarterly_points = df.apply(calculate_quarter_points,
                                axis=1, args=(colomn,))

    quarterly_points = [
        item for sublist in quarterly_points for item in sublist
    ]

    quarterly_df = pd.DataFrame(quarterly_points)

    result = quarterly_df.groupby('Квартал')[f'{colomn}'].sum().reset_index()
    result['Квартал'] = result['Квартал'].apply(
        lambda x: f"{x.quarter}-{x.year}"
        )

    return result[result['Квартал'].str.contains(f'{dt.now().year}')]
