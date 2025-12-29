import pandas as pd


def check_add_filled_projects(row: pd.Series) -> bool:
    characteristics = [
        "Наименование объекта",
        "Тип работы",
        "Дата начала проекта",
    ]
    for char in characteristics:
        if row[f"{char}"] == "" or row[f"{char}"] is None:
            return False
    return True
