# analysis.py
import duckdb
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

DB_PATH = "heatgrid.duckdb"
FIG_DIR = Path("figures")
FIG_DIR.mkdir(exist_ok=True)

def load_data():
    """
    Load the joined daily dataset from DuckDB.

    Table: heat_load_daily
    Columns we expect:
      - station (IAD, BOS)
      - region (PJM, ISNE)
      - day_utc
      - daily_max_temp_C
      - avg_temp_C
      - t90_max
      - is_hot_day  (0/1/NULL or boolean-ish)
      - daily_total_mwh
      - daily_peak_mwh
    """
    con = duckdb.connect(DB_PATH, read_only=True)
    df = con.execute("""
        SELECT
            station,
            region,
            day_utc,
            daily_max_temp_C,
            avg_temp_C,
            t90_max,
            is_hot_day,
            daily_total_mwh,
            daily_peak_mwh
        FROM heat_load_daily
        ORDER BY region, day_utc;
    """).fetchdf()
    con.close()

    # Ensure datetime
    df["day_utc"] = pd.to_datetime(df["day_utc"])

    # Make sure numeric columns are numeric
    for col in ["daily_max_temp_C", "avg_temp_C", "t90_max",
                "daily_total_mwh", "daily_peak_mwh"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Normalize is_hot_day -> clean boolean
    # Anything truthy -> True (hot), anything else -> False (normal)
    df["is_hot_bool"] = df["is_hot_day"].fillna(0).astype(int).astype(bool)

    return df


def plot_avg_load_hot_vs_normal(df: pd.DataFrame):
    """
    Compare **average daily total load** on hot vs normal days per region.

    Uses:
      - is_hot_bool     (True/False)
      - daily_total_mwh (total energy used that day in MWh)
      - region          (PJM, ISNE)
    """
    sub = df.copy()

    # Keep only rows where we actually have load data
    sub = sub.dropna(subset=["daily_total_mwh"])
    if sub.empty:
        print("[WARN] No rows with daily_total_mwh; skipping hot vs normal plot.")
        return

    # Label "Hot days" / "Normal days" based on boolean
    sub["label"] = sub["is_hot_bool"].map(
        {True: "Hot days", False: "Normal days"}
    )

    # Group by region + label and compute mean daily load
    agg = (
        sub.groupby(["region", "label"], as_index=False)["daily_total_mwh"]
           .mean()
           .rename(columns={"daily_total_mwh": "avg_daily_mwh"})
    )

    print("\n[DEBUG] agg for hot vs normal:")
    print(agg)

    if agg.empty:
        print("[WARN] Aggregated data is empty; nothing to plot hot vs normal.")
        return

    # Pivot to wide format: rows = region, columns = label
    pivot = agg.pivot(index="region", columns="label", values="avg_daily_mwh")

    ax = pivot.plot(kind="bar")
    ax.set_title("Average Daily Electricity Load: Hot vs Normal Days")
    ax.set_xlabel("Region")
    ax.set_ylabel("Avg daily load (MWh)")
    ax.legend(title="Day type")

    plt.tight_layout()
    out = FIG_DIR / "avg_load_hot_vs_normal.png"
    plt.savefig(out)
    plt.close()
    print(f"✓ Saved: {out}")


def plot_temp_vs_load_scatter(df: pd.DataFrame, region: str):
    """
    For one region (PJM or ISNE), show how daily max temperature
    relates to daily total load, coloring heatwave vs normal days.
    """
    sub = df[
        (df["region"] == region)
        & df["daily_total_mwh"].notna()
        & df["daily_max_temp_C"].notna()
    ].copy()

    if sub.empty:
        print(f"[WARN] No data for region={region}; skipping scatter.")
        return

    colors = sub["is_hot_bool"].map({False: "blue", True: "red"})

    plt.figure(figsize=(7, 5))
    plt.scatter(sub["daily_max_temp_C"], sub["daily_total_mwh"],
                c=colors, alpha=0.6)

    plt.title(f"{region}: Daily Max Temperature vs Total Load")
    plt.xlabel("Daily max temperature (°C)")
    plt.ylabel("Daily total load (MWh)")

    import matplotlib.lines as mlines
    legend = [
        mlines.Line2D([], [], color="blue", marker="o", linestyle="None",
                      label="Normal day"),
        mlines.Line2D([], [], color="red", marker="o", linestyle="None",
                      label="Heatwave day"),
    ]
    plt.legend(handles=legend)

    plt.tight_layout()
    out = FIG_DIR / f"scatter_temp_vs_load_{region}.png"
    plt.savefig(out)
    plt.close()
    print(f"✓ Saved: {out}")


def plot_time_series_with_heatwaves(df: pd.DataFrame, station: str):
    """
    Time series of daily load for a single station's region
    (IAD -> PJM, BOS -> ISNE) with heatwave days highlighted.
    """
    sub = df[df["station"] == station].copy()
    sub = sub[sub["daily_total_mwh"].notna()]

    if sub.empty:
        print(f"[WARN] No data for station={station}; skipping time series.")
        return

    plt.figure(figsize=(12, 5))
    plt.plot(sub["day_utc"], sub["daily_total_mwh"],
             color="gray", label="Daily load")

    hw = sub[sub["is_hot_bool"]]
    if not hw.empty:
        plt.scatter(hw["day_utc"], hw["daily_total_mwh"],
                    color="red", s=10, label="Heatwave day")

    plt.title(f"{station} Region: Daily Load with Heatwave Days Highlighted")
    plt.xlabel("Date")
    plt.ylabel("Daily load (MWh)")
    plt.legend()
    plt.tight_layout()

    out = FIG_DIR / f"time_series_heatwaves_{station}.png"
    plt.savefig(out)
    plt.close()
    print(f"✓ Saved: {out}")


def main():
    print("Loading heat_load_daily...")
    df = load_data()

    print("Generating figures...")
    # 1) Average load on hot vs normal days, by region
    plot_avg_load_hot_vs_normal(df)

    # 2) Scatter plots for each region
    for region in ["PJM", "ISNE"]:
        plot_temp_vs_load_scatter(df, region)

    # 3) Time series for each station
    for station in ["IAD", "BOS"]:
        plot_time_series_with_heatwaves(df, station)


if __name__ == "__main__":
    main()
