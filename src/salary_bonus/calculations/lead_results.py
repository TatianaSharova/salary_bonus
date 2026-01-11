import pandas as pd

from src.salary_bonus.worksheets.worksheets import send_lead_res_to_ws


def collect_lead_results(
    eng_points: dict[str, pd.DataFrame], leads: dict[str, list[str]]
) -> dict[str, pd.DataFrame]:
    """Собирает результаты по руководителям группы."""
    result: dict[str, pd.DataFrame] = {}

    # все месяцы, которые встречаются в данных
    all_months: set[str] = set()
    for df in eng_points.values():
        all_months.update(df["Месяц"].unique())

    # сортировка MM-YYYY
    all_months = sorted(all_months, key=lambda x: (x.split("-")[1], x.split("-")[0]))

    for lead_name, engineers in leads.items():
        rows = []

        for engineer in engineers:
            row = {month: 0.0 for month in all_months}
            row["Имя"] = engineer

            df = eng_points.get(engineer)
            if df is not None:
                for _, eng_row in df.iterrows():
                    row[eng_row["Месяц"]] += float(eng_row["Баллы"])

            rows.append(row)

        lead_df = pd.DataFrame(rows).set_index("Имя")

        # итог по строке (сумма по месяцам)
        lead_df["Итого"] = lead_df.sum(axis=1)

        # итог по столбцу
        total_row = lead_df.sum(axis=0)
        lead_df.loc["Всего"] = total_row

        result[lead_name] = lead_df

    print(result)

    return result


def process_lead_data(eng_points: dict[str, pd.DataFrame], leads: dict[str, list[str]]):
    """Обрабатывает и отправляет данные по руководителям группы."""
    lead_results = collect_lead_results(eng_points, leads)
    send_lead_res_to_ws(lead_results)
