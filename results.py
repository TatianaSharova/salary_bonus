from datetime import datetime as dt

import pandas as pd
from pandas.core.frame import DataFrame
from worksheets import send_results_data_ws, send_bonus_data_ws



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
    if pd.notna(row['Баллы']) and pd.notna(row['Средние баллы/План']) and (
        row['Баллы'] >= row['Средние баллы/План']):
        return row['Баллы'] - row['Средние баллы/План']
    if pd.notna(row['Баллы']) and pd.notna(row['Средние баллы/План']) and (
        row['Баллы'] < row['Средние баллы/План']):
        return 0
    else:
        return None


def count_percent(row):
    '''Расчитывает процент от плана.'''
    if pd.notna(row['Баллы']) and pd.notna(row['Средние баллы/План']) and(
    row['Средние баллы/План'] != 0):
        percent = int((row['Баллы']/row['Средние баллы/План'])*100)
        return f'{percent} %'
    else:
        return None


def do_results(results: dict):
    '''
    Отправляет данные о плане и премиальных баллах в таблицу.
    '''
    average_df = make_results(results)

    for key, value in results.items():
        result_df = pd.merge(average_df, value, on='Квартал', how='outer')
        result_df['Премиальные баллы'] = result_df.apply(calculate_bonus, axis=1)
        result_df['Премиальные баллы'] = result_df['Премиальные баллы'].replace({pd.NA: None, float('nan'): None})
        result_df['Процент от плана'] = result_df.apply(count_percent, axis=1)
        result_df['Процент от плана'] = result_df['Процент от плана'].replace({pd.NA: None, float('nan'): None})
        result_df = result_df[['Премиальные баллы', 'Процент от плана']]

        send_bonus_data_ws(key, result_df)

    send_results_data_ws(average_df)




