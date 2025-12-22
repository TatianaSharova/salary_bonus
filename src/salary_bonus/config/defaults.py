from datetime import datetime as dt

MONTHS = {
    "1": f"Январь {dt.now().year}",
    "2": f"Февраль {dt.now().year}",
    "3": f"Март {dt.now().year}",
    "4": f"Апрель {dt.now().year}",
    "5": f"Май {dt.now().year}",
    "6": f"Июнь {dt.now().year}",
    "7": f"Июль {dt.now().year}",
    "8": f"Август {dt.now().year}",
    "9": f"Сентябрь {dt.now().year}",
    "10": f"Октябрь {dt.now().year}",
    "11": f"Ноябрь {dt.now().year}",
    "12": f"Декабрь {dt.now().year}",
}

# ws formating
ROWS_COUNT = 200
COLOMNS_COUNT = 20
ENG_WS_COL_NAMES = [
    "Страна",
    "Наименование объекта",
    "Шифр (ИСП)",
    "Разработал",
    "Баллы",
    "Дата начала проекта",
    "Дата окончания проекта",
    "Дедлайн",
    "Автоматически определенная сложность",
]

# sleep time in seconds
AFTER_FORMAT_SLEEP = 5
AFTER_ENG_SLEEP = 10

# worksheet/spreadsheet names
PROJECT_ARCHIVE = "Таблица проектов"
ADDITIONAL_WORK = "Таблица доп. работ"
SETTINGS_WS = "настройки1"
BONUS_WS = f"1Премирование{dt.now().year}"
ARCHIVE_CURRENT_WS = f"{dt.now().year}"
FIRST_SHEET = "Sheet1"
RESULT_WS = "Итоги"
