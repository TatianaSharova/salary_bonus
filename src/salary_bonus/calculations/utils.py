import pandas as pd

from src.salary_bonus.calculations.mounth_points import calculate_by_month
from src.salary_bonus.logger import logging


def find_sum_equipment(df: pd.DataFrame) -> pd.DataFrame:
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
