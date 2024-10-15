import time
from datetime import datetime as dt

import pandas as pd
from pandas.core.frame import DataFrame
from pandas.core.series import Series

from utils import MONTHS
from worksheets import (connect_to_attendance_sheet, connect_to_engineer_ws,
                        send_bonus_data_ws, send_hours_data_ws,
                        send_non_working_hours_ws, send_results_data_ws)


def count_average_points(res: dict) -> DataFrame:
    '''
    Считает среднее арифметическое количество набранных баллов в кварталах.
    '''
    merged_df = pd.concat(res.values(), ignore_index=True)

    filtered_df = merged_df[merged_df['Квартал'].str.contains(
        f'{dt.now().year}')]

    average_df = filtered_df.groupby('Квартал').mean().reset_index()
    average_df['Баллы'] = average_df['Баллы'].apply(lambda x: int(x))

    average_df = average_df.rename(columns={'Баллы': 'Средние баллы/План'})

    return average_df[['Квартал', 'Средние баллы/План']]


def calculate_bonus(row: Series) -> int:
    '''Расчитывает премиальные баллы.'''
    if pd.notna(row['Баллы']) and pd.notna(row['План']) and (
        row['Баллы'] >= row['План']
    ):
        return row['Баллы'] - row['План']
    if pd.notna(row['Баллы']) and pd.notna(row['План']) and (
        row['Баллы'] < row['План']
    ):
        return 0
    return None


def count_percent(row: Series) -> str:
    '''Расчитывает процент от плана.'''
    if pd.notna(row['Баллы']) and pd.notna(row['План']) and (
        row['Средние баллы/План'] != 0
        and row['План'] != 0
    ):
        percent = int((row['Баллы']/row['План'])*100)
        return f'{percent} %'
    return None


def count_quaterly_hours(df: DataFrame):
    '''Суммирует часы по кварталам.'''
    quarters = {
        f'1-{dt.now().year}': [MONTHS['1'], MONTHS['2'], MONTHS['3']],
        f'2-{dt.now().year}': [MONTHS['4'], MONTHS['5'], MONTHS['6']],
        f'3-{dt.now().year}': [MONTHS['7'], MONTHS['8'], MONTHS['9']],
        f'4-{dt.now().year}': [MONTHS['10'], MONTHS['11'], MONTHS['12']]
    }

    quarterly_hours = pd.DataFrame()

    for quarter, months in quarters.items():
        quarterly_hours[quarter] = df[months].sum(axis=1, skipna=True)

    quarterly_hours['Имя'] = df['Имя']

    quarterly_hours = quarterly_hours[[
        'Имя', f'1-{dt.now().year}', f'2-{dt.now().year}',
        f'3-{dt.now().year}', f'4-{dt.now().year}']]

    return quarterly_hours


def get_working_hours_data(engineers: list[str]) -> DataFrame:
    '''Собирает данные о рабочих часах проектировщиков.'''
    current_month = dt.now().month

    months_list = list(MONTHS.values())
    columns = ['Имя'] + months_list
    df = pd.DataFrame(columns=columns)

    for engineer in engineers:
        num = 1
        engineer_work = {'Имя': f'{engineer}'}
        while num <= current_month:
            worksheet = connect_to_attendance_sheet(MONTHS[f'{num}'])
            if worksheet:
                raw_data = worksheet.get('A1:T136')
                data = pd.DataFrame(raw_data[1:], columns=raw_data[0])
                filtered_df = data[data['Фамилия Имя Отчество '].str.contains(
                    f'{engineer}', na=False)]
                if not filtered_df.empty:
                    hours = filtered_df['Часы'].values[0]
                    engineer_work[f'{MONTHS[str(num)]}'] = hours
            num += 1
        df = pd.concat([df, pd.DataFrame([engineer_work])], ignore_index=True)
        time.sleep(5)

    df.iloc[:, 1:] = df.iloc[:, 1:].apply(pd.to_numeric, errors='coerce')

    max_values = df.iloc[:, 1:].max(skipna=True)
    row_with_plan = pd.DataFrame([
        ['Рабочий план (часы)'] + max_values.tolist()
    ], columns=df.columns)
    df_work = pd.concat([df, row_with_plan], ignore_index=True)

    df_work_without_nan = df_work.fillna(0)

    send_hours_data_ws(df_work_without_nan)

    return count_quaterly_hours(df_work)


def get_target(row: Series):
    '''Считает рабочий план в процентном соотношении от рабочего времени.'''
    if (pd.notna(row['Средние баллы/План'])
            and pd.notna(row['Нерабочие часы'])
            and row['Рабочие часы'] != 0):
        part_from_work_hrs = (
            int(row['Рабочие часы']) - int(row['Нерабочие часы'])
            )/int(row['Рабочие часы'])
        target = part_from_work_hrs*int(row['Средние баллы/План'])
        return int(target)
    if pd.notna(row['Средние баллы/План']) and row['Нерабочие часы'] is None:
        return int(row['Средние баллы/План'])
    if row['Средние баллы/План'] == 0:
        return 0
    return None


def search_for_hours(engineer: str, average_df: DataFrame) -> DataFrame:
    '''
    Берет информацию о нерабочем времени проектировщика из его листа
    и корректирует рабочий план в том случае,
    если информации о проектировщике нет в табеле посещаемости офиса.
    '''
    worksheet = connect_to_engineer_ws(engineer)

    if worksheet:
        raw_data = worksheet.get('Q1:Q5')
        non_working_hours_per_quarter = pd.DataFrame(
            raw_data[1:], columns=raw_data[0])
        if not non_working_hours_per_quarter.empty:
            average_df['Нерабочие часы'] = non_working_hours_per_quarter[
                'Отпуск/отгул/не работал(а) (в часах)'
            ]
            average_df['Нерабочие часы'] = average_df[
                'Нерабочие часы'].replace({pd.NA: None, float('nan'): None})
            average_df['План'] = average_df.apply(get_target, axis=1)
            average_df['План'] = average_df['План'].replace(
                {pd.NA: None, float('nan'): None})
            return average_df
        average_df['План'] = average_df['Средние баллы/План']
        average_df['План'] = average_df['План'].replace(
            {pd.NA: None, float('nan'): None})
        return average_df
    return None


def count_target(engineer: str,
                 average_df: DataFrame,
                 df_hours: DataFrame) -> DataFrame:
    '''Рассчитывает рабочий план в зависимости от рабочих часов.'''
    filtered_df = df_hours[df_hours['Имя'] == engineer]

    if (filtered_df.iloc[0, 1:] == 0).all():
        return search_for_hours(engineer, average_df)

    transposed_df = filtered_df.melt(id_vars=['Имя'],
                                     var_name='Квартал',
                                     value_name='Часы')
    transposed_df = transposed_df[['Квартал', 'Часы']]
    new_df = transposed_df.rename(columns={'Часы': f'{engineer}'})

    average_df['Нерабочие часы'] = average_df[
        'Рабочие часы'
        ] - new_df[f'{engineer}']

    average_df['План'] = average_df.apply(get_target, axis=1)

    send_non_working_hours_ws(engineer, average_df)

    return average_df


def check_none(row: Series) -> None:
    '''Проверяет, являются ли столбцы 'Премиальные баллы' и 'Процент от плана'
    пустыми. Если они пустые, то столбец 'План' тоже становится пустым(None).
    '''
    if row['Премиальные баллы'] is None and row['Процент от плана'] is None:
        return None
    return row['План']


def do_results(results: dict) -> None:
    '''
    Отправляет данные о плане и премиальных баллах в таблицу.
    '''
    average_df = count_average_points(results)
    send_results_data_ws(average_df)

    engineers = list(results.keys())
    working_hours_per_quarter = get_working_hours_data(engineers)

    full_time_work_hours = pd.DataFrame(
        {'Квартал': [f'1-{dt.now().year}', f'2-{dt.now().year}',
                     f'3-{dt.now().year}', f'4-{dt.now().year}'],
         'Рабочие часы': working_hours_per_quarter.iloc[-1, 1:].tolist()})

    target_with_hours_df = pd.merge(
        average_df, full_time_work_hours, on='Квартал', how='outer'
    )

    for key, value in results.items():
        target_df = count_target(
            key, target_with_hours_df, working_hours_per_quarter
        )
        if key == 'Трифонова' or key == 'Фокина':
            print(key)
            print(target_df)
        result_df = pd.merge(target_df, value, on='Квартал', how='outer')
        if key == 'Трифонова' or key == 'Фокина':
            print(key)
            print(result_df)
        result_df['Премиальные баллы'] = result_df.apply(
            calculate_bonus, axis=1)

        result_df['Премиальные баллы'] = result_df[
            'Премиальные баллы'].replace({pd.NA: None, float('nan'): None})

        result_df['Процент от плана'] = result_df.apply(count_percent, axis=1)

        result_df['Процент от плана'] = result_df['Процент от плана'].replace(
            {pd.NA: None, float('nan'): None})

        result_df = result_df[['План', 'Премиальные баллы',
                               'Процент от плана']]

        if key == 'Трифонова' or key == 'Фокина':
            print(key)
            print(result_df)
        result_df['План'] = result_df.apply(check_none, axis=1)
        if key == 'Трифонова' or key == 'Фокина':
            print(key)
            print(result_df)
        result_df['План'] = result_df['План'].replace(
            {pd.NA: None, float('nan'): None}
            )

        send_bonus_data_ws(key, result_df)
