import asyncio
import subprocess
import time
from datetime import datetime, timedelta

import aiogram
import gspread
import pandas as pd
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from pandas.core.frame import DataFrame
from pytz import timezone

from complexity import set_project_complexity
from counting_points import count_points
from logger import logging
from quaterly_points import calculate_quarter
from results import do_results
from utils import TELEGRAM_TOKEN, get_list_of_engineers, is_point, send_message
from worksheets import (connect_to_engineer_ws, connect_to_project_archive,
                        send_project_data_to_spreadsheet,
                        send_quarter_data_to_spreadsheet)

pd.options.mode.chained_assignment = None
pd.set_option('future.no_silent_downcasting', True)


def correct_complexity(engineer: str,
                       engineer_projects: DataFrame) -> DataFrame:
    '''
    Берёт данные о корректировке сложности из таблицы проектировщика
    и заменяет неверные данные.
    '''
    logging.info('Проверка на необходимость корректировки сложности проектов.')
    worksheet = connect_to_engineer_ws(engineer)

    if worksheet:
        raw_data = worksheet.get("J1:J200")
        new_coplexity = pd.DataFrame(raw_data[1:], columns=raw_data[0])
        if not new_coplexity.empty:
            engineer_projects[
                'Корректировка сложности'
            ] = new_coplexity['Корректировка сложности']
            if 'Корректировка сложности' in engineer_projects.columns:
                engineer_projects['Сложность для расчета'] = engineer_projects[
                    'Корректировка сложности'
                ].combine_first(engineer_projects['Сложность для расчета'])
                logging.info('Скорректировали сложность проектов '
                             'по данным с листа проектировщика.')
        logging.info('Корректировка сложности не нужна.')
    return engineer_projects


def find_sum_equipment(df: DataFrame) -> DataFrame:
    '''
    Считает сумму заложенного оборудования по кварталам.
    '''
    logging.info('Начинаем подсчет суммы заложенного оборудования по кварталам.')
    name = 'Сумма заложенного оборудования'
    df[name] = df[name].str.replace('\xa0', '').str.replace(',', '.')
    df[name] = pd.to_numeric(df[name], errors='coerce')

    equipment_df = df[[
            'Шифр (ИСП)', 'Разработал', 'Дата начала проекта',
            'Дата окончания проекта', 'Сумма заложенного оборудования'
        ]]
    equipment_df_filt = equipment_df.dropna(subset=[name])
    equipment_df_filt = equipment_df_filt[equipment_df_filt['Шифр (ИСП)'] != '']  # noqa: E501

    quaters = calculate_quarter(equipment_df_filt, colomn=name)
    quaters[name] = quaters[name].apply(
        lambda x: '{:,.2f}'.format(x).replace(',', ' ')
    )
    return quaters


def process_data(engineers: list[str], df: DataFrame) -> None:
    '''
    Собирает данные из архива проектов, производит расчет баллов
    и отправляет полученные данные в таблицу "Премирование"

    Для тех проектировщиков, для которых удалось посчитать баллы за проекты,
    выводится информация с данными о баллах по кварталам на его листе,
    а также происходит сбор информации о рабочих часах с листа посещаемости.
    '''
    results = {}

    for engineer in engineers:
        logging.info(f'Начинается расчет баллов для проектировщика {engineer}.')
        blocks = []
        engineer_projects = df.loc[
            df['Разработал'].str.contains(f'{engineer}')
        ].reset_index(drop=True)
        engineer_projects['Дедлайн'] = ''

        logging.info('Определение сложности проектов.')
        engineer_projects['Автоматически определенная сложность'] = engineer_projects.apply(  # noqa: E501
            set_project_complexity, axis=1
        )
        engineer_projects['Сложность для расчета'] = engineer_projects[
            'Автоматически определенная сложность'
        ]
        engineer_projects = correct_complexity(engineer, engineer_projects)

        logging.info('Подсчет баллов за проекты.')
        engineer_projects['Баллы'] = engineer_projects.apply(
            count_points, axis=1, args=(engineer_projects, blocks)
        )
        send_project_data_to_spreadsheet(engineer_projects, engineer)

        engineer_projects_filt = engineer_projects[
            engineer_projects['Баллы'].apply(is_point)
        ]

        engineer_projects_filtered = engineer_projects_filt[[
            'Шифр (ИСП)', 'Разработал', 'Дата начала проекта',
            'Дата окончания проекта', 'Баллы'
        ]]

        if not engineer_projects_filtered.empty:
            quarters = calculate_quarter(engineer_projects_filtered,
                                         colomn='Баллы')
            send_quarter_data_to_spreadsheet(quarters, engineer)
            results[engineer] = quarters
            logging.info(f'Расчет баллов для проектировщика {engineer} завершен. '
                         f'Ждем 10 секунд.')
        else:
            logging.info(f'Нет готовых проектов у проектировщика {engineer}. '
                         f'Переходим к следующему проектировщику через 10 секунд.')
        time.sleep(10)

    sum_equipment = find_sum_equipment(df)

    do_results(results, sum_equipment)


async def main() -> None:
    '''
    Запускает и завершает работу программы.
    '''
    logging.info('Запущена основная задача.')
    bot = aiogram.Bot(token=TELEGRAM_TOKEN)

    try:
        worksheet = connect_to_project_archive()
    except gspread.exceptions.SpreadsheetNotFound as err:
        logging.exception(err)
        await send_message(bot,
                           'Ошибка: таблица "Таблица проектов" не найдена.\n'
                           'Возможно название было сменено.')
        await bot.session.close()
        return

    df = pd.DataFrame(worksheet.get_all_records(numericise_ignore=['all']))

    if not df.empty:
        try:
            list_of_engineers = get_list_of_engineers(df, colomn='Разработал')
            logging.info(f'Список проектировщиков, для которых нужен расчет: '
                         f'{list_of_engineers}')
            if list_of_engineers != []:
                process_data(list_of_engineers, df)
                await send_message(bot, 'Расчет баллов успешно выполнен.')
                logging.info('Программа успешно выполнила работу.')
        except gspread.exceptions.APIError as error:
            logging.exception(error)
            await send_message(bot, f'{error}')
        except Exception as error:
            logging.exception(error)
            await send_message(bot, f'Ошибка: {error}')

    await bot.session.close()


async def update_holidays_package():
    '''
    Обновляет пакет holidays для подгрузки данных о выходных в новых годах.
    '''
    try:
        subprocess.run(["pip", "install", "--upgrade", "holidays"],
                       check=True)
        logging.info('Библиотека holidays была обновлена.')
    except subprocess.CalledProcessError as error:
        logging.exception(f'Библиотека holidays не обновлена: {error}')
        await send_message(aiogram.Bot(token=TELEGRAM_TOKEN),
                           f'Ошибка при обновлении holidays library: {error}')


def setup_scheduler():
    '''
    Запускает планировщик. Задача выполнится сразу после запуска,
    а потом будет выполняться по вт и пт в 10:00 утра.
    '''
    scheduler = AsyncIOScheduler(timezone='Asia/Dubai')

    samara_tz = timezone('Asia/Dubai')

    scheduler.add_job(
        main, trigger='date',
        next_run_time=datetime.now(samara_tz)+timedelta(seconds=5),
        misfire_grace_time=120)

    scheduler.add_job(
        main, CronTrigger(day_of_week='tue,fri',
                          hour=10, minute=0, timezone=samara_tz),
        misfire_grace_time=60)

    scheduler.add_job(
        update_holidays_package,
        'cron', month=12, day=1, hour=9, minute=0,
        misfire_grace_time=60, timezone=samara_tz)

    scheduler.start()


if __name__ == "__main__":
    setup_scheduler()

    try:
        asyncio.get_event_loop().run_forever()
    except (KeyboardInterrupt, SystemExit) as err:
        logging.critical(err)
