from datetime import datetime as dt

import pandas as pd
import holidays
from pandas.core.frame import DataFrame
from worksheets import send_results_data_ws, send_bonus_data_ws, connect_to_engineer_ws
from pandas.core.series import Series



def make_results(res: dict) -> DataFrame:
    '''
    Считает среднее арифметическое количество набранных баллов в кварталах.
    '''
    merged_df = pd.concat(res.values(), ignore_index=True)

    filtered_df = merged_df[merged_df['Квартал'].str.contains(f'{dt.now().year}')]

    average_df = filtered_df.groupby('Квартал').mean().reset_index()
    average_df['Баллы'] = average_df['Баллы'].apply(lambda x: int(x))

    average_df = average_df.rename(columns={'Баллы': 'Средние баллы/План'})

    average_df = average_df[['Квартал', 'Средние баллы/План']]

    return average_df


def calculate_bonus(row):
    '''Расчитывает премиальные баллы.'''
    if pd.notna(row['Баллы']) and pd.notna(row['План']) and (
        row['Баллы'] >= row['План']):
        return row['Баллы'] - row['План']
    if pd.notna(row['Баллы']) and pd.notna(row['План']) and (
        row['Баллы'] < row['План']):
        return 0
    else:
        return None


def count_percent(row):
    '''Расчитывает процент от плана.'''
    if pd.notna(row['Баллы']) and pd.notna(row['План']) and(
    row['Средние баллы/План'] != 0):
        percent = int((row['Баллы']/row['План'])*100)
        return f'{percent} %'
    else:
        return None


def count_working_hours_per_quarter(year: int) -> DataFrame:
    '''
    Считает количество рабочих часов в каждом квартале в заданном году с учетом праздников и выходных.
    '''
    ru_holidays = holidays.RU(years=year)
    
    date_range = pd.date_range(start=f'{year}-01-01', end=f'{year}-12-31', freq='D')

    dates = pd.Series(date_range.date)

    working_days = date_range[(date_range.weekday < 5) & (~dates.isin(ru_holidays))]

    quarters = {
        f'1-{year}': working_days[working_days.quarter == 1],
        f'2-{year}': working_days[working_days.quarter == 2],
        f'3-{year}': working_days[working_days.quarter == 3],
        f'4-{year}': working_days[working_days.quarter == 4]
    }

    working_hours_per_quarter = {q: len(days) * 8 for q, days in quarters.items()}

    df_working_hours_per_quarter = pd.DataFrame(
        working_hours_per_quarter.items(),
        columns=['Квартал', 'Рабочие часы']
    )

    return df_working_hours_per_quarter


def get_target(row: Series):
    '''Считает рабочий план, исходя от рабочего времени.'''
    if pd.notna(row['Средние баллы/План']) and row['Нерабочие часы'] is not None:
        part_from_work_hrs = (int(row['Рабочие часы']) - int(row['Нерабочие часы']))/int(row['Рабочие часы'])
        target = part_from_work_hrs*int(row['Средние баллы/План'])
        return int(target)
    if pd.notna(row['Средние баллы/План']) and row['Нерабочие часы'] is None:
        return int(row['Средние баллы/План'])
    else:
        return None


def count_target(engineer: str, average_df: DataFrame) -> DataFrame:
    '''
    Берет информацию о нерабочем времени проектировщика из его листа и корректирует рабочий план.
    '''
    worksheet = connect_to_engineer_ws(engineer)

    if worksheet:
        raw_data = worksheet.get('Q1:Q5')
        non_working_hours_per_quarter = pd.DataFrame(raw_data[1:], columns=raw_data[0])
        if not non_working_hours_per_quarter.empty:
            average_df['Нерабочие часы'] = non_working_hours_per_quarter['Отпуск/отгул (в часах)']
            average_df['Нерабочие часы'] = average_df['Нерабочие часы'].replace({pd.NA: None, float('nan'): None})
            average_df['План'] = average_df.apply(get_target, axis=1)
            average_df['План'] = average_df['План'].replace({pd.NA: None, float('nan'): None})
            return average_df
        average_df['План'] = average_df['Средние баллы/План']
        average_df['План'] = average_df['План'].replace({pd.NA: None, float('nan'): None})
        return average_df


def do_results(results: dict):
    '''
    Отправляет данные о плане и премиальных баллах в таблицу.
    '''
    average_df = make_results(results)
    send_results_data_ws(average_df)

    working_hours_per_quarter = count_working_hours_per_quarter(int(f'{dt.now().year}'))

    target_with_hours_df = pd.merge(average_df, working_hours_per_quarter, on='Квартал', how='outer')

    for key, value in results.items():
        target_df = count_target(key, target_with_hours_df)
        result_df = pd.merge(target_df, value, on='Квартал', how='outer')
        result_df['Премиальные баллы'] = result_df.apply(calculate_bonus, axis=1)
        result_df['Премиальные баллы'] = result_df['Премиальные баллы'].replace({pd.NA: None, float('nan'): None})
        result_df['Процент от плана'] = result_df.apply(count_percent, axis=1)
        result_df['Процент от плана'] = result_df['Процент от плана'].replace({pd.NA: None, float('nan'): None})
        result_df = result_df[['План', 'Премиальные баллы', 'Процент от плана']]
        print(result_df)

        send_bonus_data_ws(key, result_df)
