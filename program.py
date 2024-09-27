import asyncio
import time
from datetime import datetime, timedelta

import aiogram
import gspread
import pandas as pd
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from gspread.worksheet import Worksheet
from gspread_formatting import *
from pandas.core.frame import DataFrame
from pytz import timezone

from complexity import set_project_complexity
from counting_points import count_adjusting_points, count_points
from exceptions import TooManyRequestsApiError
from quaterly_points import calculate_quarter
from utils import TELEGRAM_TOKEN, get_list_of_engineers, is_point, send_message
from worksheets import (connect_to_project_archive,
                        send_adj_data_to_spreadsheet,
                        send_project_data_to_spreadsheet,
                        send_quarter_data_to_spreadsheet)

pd.options.mode.chained_assignment = None


def process_adjusting_data(df: DataFrame) -> Worksheet:
    '''
    Расчитывает баллы за корректировки и отсылает данные в таблицу "Премирование".
    '''
    eng_for_adj = get_list_of_engineers(df, colomn='Корректировки')
    
    if eng_for_adj != []:
        for engineer in eng_for_adj:
            adjusting_projects = df.loc[df['Корректировки'].str.contains(f'{engineer}')].reset_index(drop=True)
            adjusting_projects["Баллы"] = adjusting_projects.apply(count_adjusting_points, axis=1, args=(adjusting_projects, engineer))
            adjusting_projects_small = adjusting_projects[['Наименование объекта', 'Шифр (ИСП)', 'Разработал', 'Баллы',
                    'Корректировки']]
            send_adj_data_to_spreadsheet(adjusting_projects_small, engineer)


def process_data(engineers: list[str], df: DataFrame) -> None:
    '''
    Собирает данные из архива проектов, производит расчет баллов
    и отправляет полученные данные в таблицу "Премирование".
    '''
    for engineer in engineers:
        print(engineer)
        engineer_projects = df.loc[df['Разработал'].str.contains(f'{engineer}')].reset_index(drop=True)
        engineer_projects["Дедлайн"] = ''
        blocks = []
        engineer_projects["Автоматически определенная сложность"] = engineer_projects.apply(set_project_complexity, axis=1)
        engineer_projects["Баллы"] = engineer_projects.apply(count_points, axis=1, args=(engineer_projects, blocks))
        send_project_data_to_spreadsheet(engineer_projects, engineer)


        engineer_projects_filt = engineer_projects[engineer_projects['Баллы'].apply(is_point)]

        engineer_projects_filtered = engineer_projects_filt[[
            'Шифр (ИСП)', 'Разработал', 'Дата начала проекта', 'Дата окончания проекта', 'Баллы'
        ]]


        if not engineer_projects_filtered.empty:
            quarters = calculate_quarter(engineer_projects_filtered)
            send_quarter_data_to_spreadsheet(quarters, engineer)
        time.sleep(20)
    
    process_adjusting_data(df)
        

async def main():
    '''
    Запускает и завершает работу программы.
    '''
    bot = aiogram.Bot(token=TELEGRAM_TOKEN)
    
    try:
        worksheet = connect_to_project_archive()
    except gspread.exceptions.SpreadsheetNotFound:
        await send_message(bot, 'Ошибка: таблица "Таблица проектов" не найдена.\n'
                           'Возможно название было сменено.')

    df = pd.DataFrame(worksheet.get_all_records(numericise_ignore=['all']))

    if not df.empty:
        list_of_engineers = get_list_of_engineers(df, colomn='Разработал')
        if list_of_engineers != []:
            try:
                salary_bonus = process_data(list_of_engineers, df)

                await send_message(bot, 'Расчет баллов успешно выполнен.')
            except gspread.exceptions.APIError as error:
                await send_message(bot, f'{error}')
                raise TooManyRequestsApiError(error)
            # except Exception as error:
            #     await send_message(bot, f'Ошибка: {error}')
    
    await bot.session.close()


def setup_scheduler():
    '''
    Запускает планировщик. Задача выполнится сразу после запуска,
    а потом будет выполняться каждый день в 10:10 утра.
    '''
    scheduler = AsyncIOScheduler(timezone='Asia/Dubai')

    samara_tz = timezone('Asia/Dubai')

    scheduler.add_job(main, trigger='date',
                      next_run_time=datetime.now(samara_tz)+timedelta(seconds=5),
                      misfire_grace_time=120)

    scheduler.add_job(main, trigger='cron', hour=10, minute=10,
                      misfire_grace_time=60)

    scheduler.start()


if __name__ == "__main__":
    setup_scheduler()

    try:
        asyncio.get_event_loop().run_forever()
    except (KeyboardInterrupt, SystemExit):
        pass