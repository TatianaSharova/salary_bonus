import pandas as pd
from pandas.core.frame import DataFrame

pd.options.mode.chained_assignment = None


def calculate_quarter_points(row: DataFrame) -> list[dict]:
    '''
    Производит распределение баллов по кварталам.
    Если проект был выполнен в течение одного квартала - все баллы пойдут в этот квартал.

    Если проект был выполнен в течение нескольких кварталов, то баллы будут распределены по кварталам пропорционально времени,
    потраченному на проект в каждом квартале.

    Для блок-контейнеров считается, что они были выполнены в течение одного месяца.
    '''
    row['Дата начала проекта'] = pd.to_datetime(row['Дата начала проекта'],
                                                dayfirst=True, format='%d.%m.%Y')
    row['Дата окончания проекта'] = pd.to_datetime(row['Дата окончания проекта'],
                                                   dayfirst=True, format='%d.%m.%Y')
    
    if row['Дата начала проекта'] > row['Дата окончания проекта']:
        row['Дата начала проекта'], row['Дата окончания проекта'] = row['Дата окончания проекта'], row['Дата начала проекта']

    try:
        quarters = pd.period_range(start=row['Дата начала проекта'],
                                   end=row['Дата окончания проекта'], freq='Q')
    except ValueError:
        month = row['Шифр (ИСП)'][5:7]
        year = row['Шифр (ИСП)'][:4]
        row['Дата начала проекта'] = pd.to_datetime(f'01.{month}.{year}',
                                                    dayfirst=True, format='%d.%m.%Y')
        row['Дата окончания проекта'] = pd.to_datetime(f'28.{month}.{year}',
                                                       dayfirst=True, format='%d.%m.%Y')
        quarters = pd.period_range(start=row['Дата начала проекта'],
                                   end=row['Дата окончания проекта'], freq='Q')

    quarter_points = []

    for quarter in quarters:
        quarter_start = max(pd.Timestamp(quarter.start_time), row['Дата начала проекта'])
        quarter_end = min(pd.Timestamp(quarter.end_time), row['Дата окончания проекта'])
        days_in_quarter = (quarter_end - quarter_start).days + 1
        total_days = (row['Дата окончания проекта'] - row['Дата начала проекта']).days + 1
        proportion = days_in_quarter / total_days
        points = round(row['Баллы'] * proportion, 2)
        quarter_points.append({
            'Шифр_ИСП': row['Шифр (ИСП)'],
            'Квартал': quarter,
            'Баллы': points
        })
    return quarter_points


def calculate_quarter(df: DataFrame) -> DataFrame:
    '''
    Создает таблицу с кварталами и заработанными баллами в каждом квартале.
    '''
    quarterly_points = df.apply(calculate_quarter_points, axis=1)

    quarterly_points = [item for sublist in quarterly_points for item in sublist]

    quarterly_df = pd.DataFrame(quarterly_points)

    result = quarterly_df.groupby('Квартал')['Баллы'].sum().reset_index()
    result['Квартал'] = result['Квартал'].apply(
        lambda x: f"{x.quarter}-{x.year}"
        )

    print(result)
    return result
