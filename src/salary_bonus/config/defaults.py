from datetime import datetime as dt

CURRENT_YEAR = str(dt.now().year)
# CURRENT_YEAR = "2025"  # TODO del
CURRENT_MONTH = dt.now().month
MONTHS = {
    "1": f"Январь {CURRENT_YEAR}",
    "2": f"Февраль {CURRENT_YEAR}",
    "3": f"Март {CURRENT_YEAR}",
    "4": f"Апрель {CURRENT_YEAR}",
    "5": f"Май {CURRENT_YEAR}",
    "6": f"Июнь {CURRENT_YEAR}",
    "7": f"Июль {CURRENT_YEAR}",
    "8": f"Август {CURRENT_YEAR}",
    "9": f"Сентябрь {CURRENT_YEAR}",
    "10": f"Октябрь {CURRENT_YEAR}",
    "11": f"Ноябрь {CURRENT_YEAR}",
    "12": f"Декабрь {CURRENT_YEAR}",
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
ADD_WORK_COL_NAMES = [
    "Страна",
    "Наименование объекта",
    "Шифр проекта/Номера расчета (ТактГаз)",
    "Разработал",
    "Баллы",
    "Дата начала проекта",
    "Дата окончания проекта",
    "Дедлайн",
]

# sleep time in seconds
AFTER_FORMAT_SLEEP = 5
AFTER_ENG_SLEEP = 10

# worksheet/spreadsheet names
PROJECT_ARCHIVE = "Таблица проектов"
ADDITIONAL_WORK = "Таблица доп. работ"
SETTINGS_WS = "Настройки"
BONUS_WS = f"Премирование{CURRENT_YEAR}"  # TODO rename to normal
ARCHIVE_CURRENT_WS = CURRENT_YEAR
FIRST_SHEET = "Sheet1"
RESULT_WS = "Итоги"

# additional work types
ADD_WORK_TYPES = [
    "ГР Модульная установка",
    "ГР Модульная установка 500+",
    "ГР Централизованная установка",
    "Подготовка спецификации",
    "Расстановка оборудования",
]
