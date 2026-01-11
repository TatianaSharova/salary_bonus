from datetime import datetime as dt

import pandas as pd
from pandas.core.frame import DataFrame

from src.salary_bonus.config.defaults import CURRENT_YEAR, MONTHS
from src.salary_bonus.logger import logging
from src.salary_bonus.worksheets.worksheets import (
    get_attendance_sheet_ws,
    send_hours_data_ws,
    send_results_data_ws,
)


def count_average_points(res: dict) -> DataFrame:
    """
    Считает среднее арифметическое количество набранных баллов в кварталах.
    """
    logging.info(
        "Расчет среднего арифметического количества " "набранных баллов по кварталам."
    )
    merged_df = pd.concat(res.values(), ignore_index=True)

    filtered_df = merged_df[merged_df["Месяц"].str.contains(CURRENT_YEAR)]

    average_df = filtered_df.groupby("Месяц").mean().reset_index()
    average_df["Баллы"] = average_df["Баллы"].apply(lambda x: int(x))

    average_df = average_df.rename(columns={"Баллы": "Средний балл"})

    return average_df[["Месяц", "Средний балл"]]


def get_working_hours_data(engineers: list[str]) -> DataFrame:
    """Собирает данные о рабочих часах проектировщиков."""
    logging.info("Сбор данных о рабочих часах проектировщиков.")
    current_month = dt.now().month
    monthly_data = {}
    months_list = list(MONTHS.values())
    columns = ["Имя"] + months_list
    df = pd.DataFrame(columns=columns)

    attendance_ws_all = get_attendance_sheet_ws()

    for num in range(1, current_month + 1):
        for worksheet in attendance_ws_all:
            if worksheet.title == MONTHS[str(num)]:
                raw_data = worksheet.get("A1:T160")
                data = pd.DataFrame(raw_data[1:], columns=raw_data[0])
                monthly_data[MONTHS[str(num)]] = data

    for engineer in engineers:
        engineer_work = {"Имя": f"{engineer}"}
        for month_name, data in monthly_data.items():
            filtered_df = data[
                data["Фамилия Имя Отчество "].str.contains(f"{engineer}", na=False)
            ]
            if not filtered_df.empty:
                hours = filtered_df["Часы"].values[0]
                engineer_work[month_name] = hours
        df = pd.concat([df, pd.DataFrame([engineer_work])], ignore_index=True)

    df.iloc[:, 1:] = df.iloc[:, 1:].apply(pd.to_numeric, errors="coerce")

    max_values = df.iloc[:, 1:].max(skipna=True)
    row_with_plan = pd.DataFrame(
        [["Рабочий план (часы)"] + max_values.tolist()], columns=df.columns
    )
    df_work = pd.concat([df, row_with_plan], ignore_index=True)

    df_work_without_nan = df_work.fillna(0)

    return df_work_without_nan


def do_results(results: dict, sum_equipment: DataFrame) -> None:
    """
    Отправляет данные о плане и премиальных баллах в таблицу.
    """
    logging.info("Начинаем подсчет квартального плана и премиальных баллов.")

    # Подсчет и отправка средних баллов
    average_df = count_average_points(results)
    res_df = pd.merge(average_df, sum_equipment, on="Месяц", how="outer")
    res_df["Средний балл"] = res_df["Средний балл"].fillna(0)
    res_df["Сумма заложенного оборудования"] = res_df[
        "Сумма заложенного оборудования"
    ].fillna(0)
    send_results_data_ws(res_df)

    # Сбор и отправка рабочих часов
    engineers = list(results.keys())
    working_hours_per_quarter = get_working_hours_data(engineers)
    send_hours_data_ws(working_hours_per_quarter)
