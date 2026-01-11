import time

import pandas as pd
from pandas.core.frame import DataFrame

from src.salary_bonus.calculations.mounth_points import calculate_by_month
from src.salary_bonus.calculations.project_archive.complexity import (
    set_project_complexity,
)
from src.salary_bonus.calculations.project_archive.counting_points import count_points
from src.salary_bonus.calculations.results import do_results
from src.salary_bonus.config.defaults import AFTER_ENG_SLEEP
from src.salary_bonus.logger import logging
from src.salary_bonus.notification.telegram.bot import TelegramNotifier
from src.salary_bonus.utils import get_project_archive_data, is_point
from src.salary_bonus.worksheets.worksheets import (
    connect_to_engineer_ws,
    send_project_data_to_spreadsheet,
)


def correct_complexity(engineer: str, engineer_projects: DataFrame) -> DataFrame:
    """
    Берёт данные о корректировке сложности из таблицы проектировщика
    и заменяет неверные данные.
    """
    logging.info("Проверка на необходимость корректировки сложности проектов.")
    worksheet = connect_to_engineer_ws(engineer, False)

    if worksheet:
        raw_data = worksheet.get("J1:J200")
        new_coplexity = pd.DataFrame(raw_data[1:], columns=raw_data[0])
        if not new_coplexity.empty:
            engineer_projects["Корректировка сложности"] = new_coplexity[
                "Корректировка сложности"
            ]
            if "Корректировка сложности" in engineer_projects.columns:
                engineer_projects["Сложность для расчета"] = engineer_projects[
                    "Корректировка сложности"
                ].combine_first(engineer_projects["Сложность для расчета"])
                logging.info(
                    "Скорректировали сложность проектов "
                    "по данным с листа проектировщика."
                )
        logging.info("Корректировка сложности не нужна.")
    return engineer_projects


def find_sum_equipment(df: DataFrame) -> DataFrame:
    """
    Считает сумму заложенного оборудования по кварталам.
    """
    logging.info("Начинаем подсчет суммы заложенного оборудования по кварталам.")
    name = "Сумма заложенного оборудования"
    df[name] = df[name].str.replace("\xa0", "").str.replace(",", ".")
    df[name] = pd.to_numeric(df[name], errors="coerce")

    equipment_df = df[
        [
            "Шифр (ИСП)",
            "Разработал",
            "Дата начала проекта",
            "Дата окончания проекта",
            "Сумма заложенного оборудования",
        ]
    ]
    equipment_df_filt = equipment_df.dropna(subset=[name])
    equipment_df_filt = equipment_df_filt[
        equipment_df_filt["Шифр (ИСП)"] != ""
    ]  # noqa: E501

    quaters = calculate_by_month(equipment_df_filt, column=name)
    quaters[name] = quaters[name].apply(lambda x: "{:,.2f}".format(x).replace(",", " "))
    return quaters


async def process_project_archive_data(
    engineers: list[str], tg_bot: TelegramNotifier
) -> tuple[dict[str, DataFrame], dict[str, DataFrame]]:
    """
    Собирает данные из архива проектов, производит расчет баллов
    и отправляет полученные данные в таблицу "Премирование"

    Для тех проектировщиков, для которых удалось посчитать баллы за проекты,
    выводится информация с данными о баллах по кварталам на его листе,
    а также происходит сбор информации о рабочих часах с листа посещаемости.

    Args:
        engineers (list[str]): список инженеров, для которых надо делать расчет
        tg_bot (TelegramNotifier): тг-бот для отправки уведомлений

    Returns:
        tuple[dict[str, DataFrame], dict[str, DataFrame]]: кортеж из двух
        словарей. Первый содержит рассчитанные данные по месяцам, где key -
        проектировщик, value - DataFrame вида:
        | Месяц (pandas.Period("YYYY-MM", freq="M")) | Баллы (float) |.
        Второй содержит данные по проектам из основной таблицы, где key -
        проектировщик, value - DataFrame с исходными строками проектов.
    """
    results: dict[str, DataFrame] = {}
    eng_data: dict[str, DataFrame] = {}

    df = get_project_archive_data()

    if df is None:
        await tg_bot.send_message(
            'Ошибка: таблица "Таблица проектов" не найдена.\n'
            "Возможно название было сменено.",
        )
        return results, eng_data
    elif isinstance(df, pd.DataFrame) and df.empty:
        msg = "Основная таблица проектов пуста, расчет по ней не будет произведен."
        logging.warning(msg)
        return results, eng_data

    for engineer in engineers:
        logging.info(f"Начинается расчет баллов для проектировщика {engineer}.")
        blocks = []
        engineer_projects = df.loc[
            df["Разработал"].str.contains(f"{engineer}")
        ].reset_index(drop=True)
        engineer_projects["Дедлайн"] = ""

        logging.info("Определение сложности проектов.")
        engineer_projects["Автоматически определенная сложность"] = (
            engineer_projects.apply(set_project_complexity, axis=1)  # noqa: E501
        )
        engineer_projects["Сложность для расчета"] = engineer_projects[
            "Автоматически определенная сложность"
        ]
        engineer_projects = correct_complexity(engineer, engineer_projects)

        logging.info("Подсчет баллов за проекты.")
        engineer_projects["Баллы"] = engineer_projects.apply(
            count_points, axis=1, args=(engineer_projects, blocks)
        )

        eng_data[engineer] = engineer_projects  # записываем данные с основной таблицы
        send_project_data_to_spreadsheet(engineer_projects, engineer)

        engineer_projects_filt = engineer_projects[
            engineer_projects["Баллы"].apply(is_point)
        ]

        if not engineer_projects_filt.empty:
            months = calculate_by_month(engineer_projects_filt, column="Баллы")
            results[engineer] = months
            logging.info(
                f"Расчет баллов для проектировщика {engineer} завершен. "
                f"Ждем 10 секунд."
            )
        else:
            logging.info(
                f"Нет готовых проектов у проектировщика {engineer}. "
                f"Переходим к следующему проектировщику через 10 секунд."
            )
        time.sleep(AFTER_ENG_SLEEP)

    sum_equipment = find_sum_equipment(df)

    do_results(results, sum_equipment)  # TODO перенос этого в main для сбора всех баллов

    return results, eng_data
