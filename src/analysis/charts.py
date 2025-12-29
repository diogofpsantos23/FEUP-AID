import argparse
import re
from pathlib import Path

import numpy as np
import pandas as pd

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


def extract_query_block(text: str, query_file: str) -> str:
    m = re.search(rf"--- Running: {re.escape(query_file)} ---", text)
    if not m:
        raise ValueError(f"Could not find output for {query_file} in the input file.")
    block = text[m.end():]
    next_m = re.search(r"--- Running: query\d+\.sql ---", block)
    if next_m:
        block = block[:next_m.start()]
    return block.strip()


def parse_ascii_table(block: str) -> pd.DataFrame:
    lines = [ln.rstrip("\n") for ln in block.splitlines()]

    header_idx = None
    for i, ln in enumerate(lines):
        if "|" in ln and not ln.strip().startswith("---"):
            if i + 1 < len(lines) and re.match(r"^\s*-", lines[i + 1]) and "+" in lines[i + 1]:
                header_idx = i
                break

    if header_idx is None:
        raise ValueError("Could not locate table header in block.")

    header = [c.strip() for c in lines[header_idx].split("|")]
    data = []

    for ln in lines[header_idx + 2:]:
        if ln.strip().startswith("(") and "rows" in ln:
            break
        if ln.strip().startswith("..."):
            continue
        if "|" not in ln:
            continue

        parts = [c.strip() for c in ln.split("|")]
        if len(parts) < len(header):
            parts += [""] * (len(header) - len(parts))
        elif len(parts) > len(header):
            parts = parts[:len(header)]

        data.append(parts)

    return pd.DataFrame(data, columns=header)


def to_num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s.replace({"None": np.nan, "": np.nan}), errors="coerce")


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def charts_output_dir() -> Path:
    out = project_root() / "src/diagram"
    out.mkdir(parents=True, exist_ok=True)
    return out


def save_chart_deterministic(fig_title: str, filename: str, dpi: int = 150) -> None:
    out_path = charts_output_dir() / filename

    if out_path.exists():
        plt.close("all")
        print(f"[SKIP] {fig_title} -> already exists: {out_path.as_posix()}")
        return

    plt.savefig(out_path, dpi=dpi, bbox_inches="tight")
    plt.close("all")
    print(f"[OK]   {fig_title} -> saved: {out_path.as_posix()}")


def chart_kpis(q1: pd.DataFrame):
    q1 = q1.copy()
    q1["Trips"] = to_num(q1["Trips"])
    q1["Revenue"] = to_num(q1["Revenue"])

    total = q1[q1["FullDate"].isin(["None"])].copy()
    if not total.empty:
        total_trips = int(total["Trips"].iloc[0])
        total_revenue = float(total["Revenue"].iloc[0])
    else:
        total_trips = int(q1["Trips"].sum(skipna=True))
        total_revenue = float(q1["Revenue"].sum(skipna=True))

    avg_value = total_revenue / total_trips if total_trips else np.nan

    fig = plt.figure(figsize=(10, 2.2))
    plt.axis("off")
    fig.text(0.01, 0.85, "KPIs — July summary", fontsize=14)

    kpis = [
        ("Trips (July)", f"{total_trips}"),
        ("Total revenue (July)", f"{total_revenue:.2f}"),
        ("Average value per trip", f"{avg_value:.2f}"),
    ]

    n = len(kpis)
    for i, (label, value) in enumerate(kpis):
        x0 = 0.01 + i * (0.98 / n)
        fig.text(x0, 0.55, label, fontsize=10)
        fig.text(x0, 0.25, value, fontsize=16)

    save_chart_deterministic("KPIs — July summary", "kpis_july.png")


def chart_revenue_by_day(q1: pd.DataFrame):
    q1 = q1.copy()
    q1["Revenue"] = to_num(q1["Revenue"])

    days = q1[~q1["FullDate"].isin(["None"])].copy()
    days["FullDate"] = pd.to_datetime(days["FullDate"], errors="coerce")
    days = days.dropna(subset=["FullDate"]).sort_values("FullDate")

    plt.figure(figsize=(10, 4))
    plt.plot(days["FullDate"], days["Revenue"])
    plt.title("Revenue by day (July)")
    plt.xlabel("Day")
    plt.ylabel("Revenue (sum of total trip value)")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()

    save_chart_deterministic("Revenue by day (July)", "revenue_by_day.png")


def chart_manhattan_trips_by_hour(q2: pd.DataFrame):
    q2 = q2.copy()
    q2["Hour"] = to_num(q2["Hour"])
    q2["Trips"] = to_num(q2["Trips"])
    q2 = q2.dropna(subset=["Hour"]).sort_values("Hour")

    plt.figure(figsize=(10, 4))
    plt.bar(q2["Hour"].astype(int), q2["Trips"])
    plt.title("Manhattan: trips by hour (July)")
    plt.xlabel("Hour of day")
    plt.ylabel("Trips")
    plt.tight_layout()

    save_chart_deterministic("Manhattan: trips by hour (July)", "manhattan_trips_by_hour.png")


def chart_manhattan_avg_value_by_hour(q2: pd.DataFrame):
    q2 = q2.copy()
    q2["Hour"] = to_num(q2["Hour"])

    avg_col = None
    for c in q2.columns:
        if c.strip().lower() in ("avgtotal", "avg_total", "avg total"):
            avg_col = c
            break
    if avg_col is None and "AvgTotal" in q2.columns:
        avg_col = "AvgTotal"
    if avg_col is None:
        raise ValueError("Could not find the average value column in query2 output.")

    q2["AvgValue"] = to_num(q2[avg_col])
    q2 = q2.dropna(subset=["Hour"]).sort_values("Hour")

    plt.figure(figsize=(10, 4))
    plt.plot(q2["Hour"].astype(int), q2["AvgValue"])
    plt.title("Manhattan: average value per trip by hour (July)")
    plt.xlabel("Hour of day")
    plt.ylabel("Average value per trip")
    plt.tight_layout()

    save_chart_deterministic(
        "Manhattan: average value per trip by hour (July)",
        "manhattan_avg_value_by_hour.png",
    )


def chart_payment_type_pie_manhattan(q4: pd.DataFrame):
    q4 = q4.copy()
    q4.columns = [c.strip() for c in q4.columns]
    for c in ("Trips_Cash", "Trips_Card", "Trips_Total"):
        if c in q4.columns:
            q4[c] = to_num(q4[c])

    if "Borough" not in q4.columns:
        raise ValueError("query4 output does not contain 'Borough' column.")

    man = q4[q4["Borough"].str.lower() == "manhattan"]
    if man.empty:
        man = q4.sort_values("Trips_Total", ascending=False).head(1)

    cash = float(man["Trips_Cash"].iloc[0])
    card = float(man["Trips_Card"].iloc[0])

    plt.figure(figsize=(6, 5))
    plt.pie([cash, card], labels=["Cash", "Card"], autopct="%1.1f%%")
    plt.title("Manhattan: payment type split (July)")
    plt.tight_layout()

    save_chart_deterministic("Manhattan: payment type split (July)", "payment_split_manhattan.png")


def chart_revenue_concentration_by_zone(q5: pd.DataFrame):
    q5 = q5.copy()
    q5.columns = [c.strip() for c in q5.columns]

    for c in ("Revenue", "CumSharePct"):
        if c not in q5.columns:
            raise ValueError(f"query5 output is missing required column: {c}")
        q5[c] = to_num(q5[c])

    zone_col = "Zone" if "Zone" in q5.columns else q5.columns[0]
    q5 = q5.sort_values("Revenue", ascending=False).reset_index(drop=True)

    x = np.arange(len(q5))

    fig, ax = plt.subplots(figsize=(18, 6))
    ax.bar(x, q5["Revenue"])
    ax.set_title(
        "Revenue concentration by pickup zones (July)\n"
        "Bars = revenue per zone; line = cumulative revenue (%)"
    )
    ax.set_xlabel("Pickup zone (sorted by revenue)")
    ax.set_ylabel("Revenue (sum of total trip value)")
    ax.set_xticks(x)
    ax.set_xticklabels(q5[zone_col].astype(str), rotation=45, ha="right")

    ax2 = ax.twinx()
    ax2.plot(
        x,
        q5["CumSharePct"],
        color="orange",
        linewidth=2,
        marker="o"
    )
    ax2.set_ylabel("Cumulative revenue (%)")
    ax2.axhline(70, color="orange", linestyle="--", linewidth=1)
    ax2.set_ylim(0, 100)

    fig.subplots_adjust(left=0.08, right=0.92, bottom=0.32, top=0.85)

    save_chart_deterministic(
        "Revenue concentration by pickup zones (July)",
        "revenue_concentration_zones.png"
    )


def chart_airport_avg_value_lines(q3: pd.DataFrame):
    q3 = q3.copy()
    q3.columns = [c.strip() for c in q3.columns]

    dow_col = "DayOfWeekName" if "DayOfWeekName" in q3.columns else q3.columns[0]

    avg_col = None
    for c in q3.columns:
        if c.strip().lower() in ("avgtotal", "avg_total", "avg total"):
            avg_col = c
            break
    if avg_col is None and "AvgTotal" in q3.columns:
        avg_col = "AvgTotal"
    if avg_col is None:
        raise ValueError("Could not find average value column in query3 output.")

    q3["Hour"] = to_num(q3["Hour"])
    q3["AvgValue"] = to_num(q3[avg_col])

    pivot = (
        q3.pivot_table(index="Hour", columns=dow_col, values="AvgValue", aggfunc="mean")
        .sort_index()
    )

    plt.figure(figsize=(10, 4))
    for col in pivot.columns:
        plt.plot(pivot.index.astype(int), pivot[col], label=str(col))

    plt.title("Airport trips: average value per trip by hour (weekend nights)")
    plt.xlabel("Hour of day")
    plt.ylabel("Average value per trip")
    plt.legend(title="Day of week")
    plt.tight_layout()

    save_chart_deterministic(
        "Airport trips: average value per trip by hour (weekend nights)",
        "airport_avg_value_weekend_nights.png",
    )


def chart_revenue_change_top3_boroughs(q9: pd.DataFrame):
    q9 = q9.copy()
    q9.columns = [c.strip() for c in q9.columns]

    if "Borough" not in q9.columns:
        raise ValueError("query9 output does not contain 'Borough' column.")

    pct_col = None
    for c in q9.columns:
        if c.strip().lower() in ("pctchange_h2_vs_h1", "pctchange", "pct_change", "pct"):
            pct_col = c
            break
    if pct_col is None:
        for c in q9.columns:
            if "pct" in c.lower():
                pct_col = c
                break
    if pct_col is None:
        raise ValueError("Could not find percent-change column in query9 output.")

    q9[pct_col] = to_num(q9[pct_col])

    top_boroughs = ["Brooklyn", "Manhattan", "Queens"]
    df3 = q9[q9["Borough"].isin(top_boroughs)].dropna(subset=[pct_col]).copy()
    df3 = df3.sort_values(pct_col, ascending=False)

    plt.figure(figsize=(7, 4))
    plt.bar(df3["Borough"], df3[pct_col])
    plt.title("Revenue change: 2nd half vs 1st half of July\nTop 3 boroughs (Brooklyn, Manhattan, Queens)")
    plt.xlabel("Borough")
    plt.ylabel("Change (%)")
    plt.tight_layout()

    save_chart_deterministic(
        "Revenue change: top 3 boroughs (Brooklyn, Manhattan, Queens)",
        "revenue_change_top3_boroughs.png",
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input",
        default=str(project_root() / "src" / "analysis" / "results.txt"),
        help="Path to results.txt",
    )
    args = parser.parse_args()

    input_path = Path(args.input)
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    text = input_path.read_text(encoding="utf-8", errors="ignore")

    q1 = parse_ascii_table(extract_query_block(text, "query1.sql"))
    q2 = parse_ascii_table(extract_query_block(text, "query2.sql"))
    q3 = parse_ascii_table(extract_query_block(text, "query3.sql"))
    q4 = parse_ascii_table(extract_query_block(text, "query4.sql"))
    q5 = parse_ascii_table(extract_query_block(text, "query5.sql"))
    q9 = parse_ascii_table(extract_query_block(text, "query9.sql"))

    print(f"Saving charts into: {(charts_output_dir()).as_posix()}")
    chart_kpis(q1)
    chart_revenue_by_day(q1)
    chart_manhattan_trips_by_hour(q2)
    chart_manhattan_avg_value_by_hour(q2)
    chart_payment_type_pie_manhattan(q4)
    chart_revenue_concentration_by_zone(q5)
    chart_airport_avg_value_lines(q3)
    chart_revenue_change_top3_boroughs(q9)

    print("\nDone.")


if __name__ == "__main__":
    main()
