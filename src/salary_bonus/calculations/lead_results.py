import pandas as pd

from src.salary_bonus.logger import logging
from src.salary_bonus.worksheets.worksheets import send_lead_res_to_ws


def collect_gip_from_eng_points(eng_points: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Сбор итоговой таблицы для ГИП из данных по инженерам."""

    all_months: set[str] = set()
    for df in eng_points.values():
        if df is not None and not df.empty:
            all_months.update(df["Месяц"].unique())

    all_months = sorted(
        all_months, key=lambda x: (int(x.split("-")[1]), int(x.split("-")[0]))
    )

    total_row = {m: 0.0 for m in all_months}

    for df in eng_points.values():
        if df is None or df.empty:
            continue
        for _, r in df.iterrows():
            total_row[r["Месяц"]] += float(r["Баллы"])

    total_row["Итого"] = float(sum(total_row.values()))
    return pd.DataFrame([total_row], index=["Всего"])


def collect_gip_df(
    lead_results: dict[str, pd.DataFrame], eng_points: dict[str, pd.DataFrame]
) -> pd.DataFrame:
    """
    Итоговая таблица для ГИП: одна строка 'Всего' по месяцам + 'Итого'.

    - Если lead_results не пустой: суммируем строки 'Всего' по каждому лиду.
    - Если lead_results пустой: считаем из eng_points (по gip_engineers, если задан).
    """
    # 1) Если есть результаты лидов - считаем из них
    if lead_results:
        # Берём полный список колонок (месяцы + Итого) из объединения
        all_cols: list[str] = []
        seen = set()
        for df in lead_results.values():
            for c in df.columns.tolist():
                if c not in seen:
                    seen.add(c)
                    all_cols.append(c)

        total = pd.Series(0.0, index=all_cols)

        for df in lead_results.values():
            if "Всего" in df.index:
                total = total.add(df.loc["Всего", all_cols].astype(float), fill_value=0.0)
            else:
                total = total.add(
                    df.sum(axis=0).reindex(all_cols).astype(float), fill_value=0.0
                )

        gip_df = pd.DataFrame([total], index=["Всего"])
        return gip_df

    # 2) Если лидов нет - считаем напрямую из eng_points по всем инженерам
    return collect_gip_from_eng_points(eng_points)


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

    return result


def process_lead_data(
    eng_points: dict[str, pd.DataFrame], leads: dict[str, list[str]], gip: list[str]
):
    """Обрабатывает и отправляет данные по руководителям группы и гип."""
    if len(leads) > 0:
        lead_results = collect_lead_results(eng_points, leads)
    else:
        lead_results = {}
        logging.warning("Для расчета баллов руководителей не найдено данных.")

    if len(gip) > 0:
        gip_results = collect_gip_df(lead_results, eng_points)
    else:
        gip_results = None
        logging.warning("Для расчета баллов ГИП'а не найдено данных.")

    send_lead_res_to_ws(lead_results, gip_results)
