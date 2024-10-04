import asyncio
import time
from datetime import timedelta
from datetime import datetime as dt

import aiogram
import gspread
import pandas as pd
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from gspread.worksheet import Worksheet
from gspread_formatting import *
from pandas.core.frame import DataFrame
from pytz import timezone
from worksheets import send_results_data_ws



def make_results(res: dict) -> DataFrame:
    '''
    Считает среднее арифметическое количество набранных баллов в кварталах.
    '''
    merged_df = pd.concat(res.values(), ignore_index=True)

    filtered_df = merged_df[merged_df['Квартал'].str.contains(f'{dt.now().year}')]

    average_df = filtered_df.groupby('Квартал').mean().reset_index()

    average_df = average_df.rename(columns={'Баллы': 'Средние баллы'})

    average_df = average_df[['Квартал', 'Средние баллы']]

    return average_df


def do_results(results: dict):
    average_df = make_results(results)

    send_results_data_ws(average_df)



