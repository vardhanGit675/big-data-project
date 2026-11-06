from pathlib import Path
import json
import numpy as np
import pandas as pd

SEED = 20261028
INPUT = Path("final_datasets/sensors_data_2015-03-20_to_2026-03-27.csv")
MASK_DIR = Path("data/masks")
TRUTH_DIR = Path("data/ground_truth")

MASK_DIR.mkdir(parents=True, exist_ok=True)
TRUTH_DIR.mkdir(parents=True, exist_ok=True)

df = pd.read_csv(INPUT)
df["row_id"] = np.arange(len(df), dtype=np.int64)
df["collected_at"] = pd.to_datetime(df["collected_at"], errors="coerce")
df["value"] = pd.to_numeric(df["value"], errors="coerce")

# Eligible, finite PM2.5 observations only; never mask invalid readings.
eligible = (
    df["source"].eq("cpcb_caaqm")
    & df["parameter_name"].eq("PM2.5")
    & df["qc_eligible"].astype(str).str.lower().isin(["true", "1", "yes"])
    & df["collected_at"].notna()
    & df["value"].notna()
    & np.isfinite(df["value"])
    & df["value"].ge(0)
    & df["value"].lt(500)
)
valid = df.loc[eligible].copy()

if valid.empty:
    raise ValueError("No eligible PM2.5 observations found.")

def save_scenario(name, selected, seed, description):
    """Save hidden positions and their values in separate files."""
    selected = selected.drop_duplicates("row_id").copy()
    mask = selected[["row_id", "station_id", "collected_at"]].copy()
    mask["scenario"] = name
    mask["seed"] = seed

    truth = selected[["row_id", "station_id", "collected_at", "value"]].copy()
    truth = truth.rename(columns={"value": "ground_truth_pm25"})

    mask.to_csv(MASK_DIR / f"{name}.csv", index=False)
    truth.to_csv(TRUTH_DIR / f"{name}_truth.csv", index=False)

    return {
        "scenario": name,
        "hidden_readings": len(mask),
        "seed": seed,
        "description": description,
    }

results = []

# 1. Random missingness, sampled separately within each station.
for pct in [10, 30, 50]:
    seed = SEED + pct
    rng = np.random.default_rng(seed)
    chosen = []

    for _, group in valid.groupby("station_id", sort=True):
        n = int(round(len(group) * pct / 100))
        if n:
            indices = rng.choice(group.index.to_numpy(), size=n, replace=False)
            chosen.extend(indices.tolist())

    results.append(save_scenario(
        f"random_{pct}pct",
        valid.loc[chosen],
        seed,
        f"{pct}% random masking within each station"
    ))

# 2. Continuous block outages. One randomly located outage
# per station per duration, using elapsed clock time.
for hours in [6, 12, 24, 48]:
    seed = SEED + 1000 + hours
    rng = np.random.default_rng(seed)
    chosen_ids = []

    for _, group in valid.groupby("station_id", sort=True):
        group = group.sort_values("collected_at")
        times = group["collected_at"]
        if len(group) == 0:
            continue

        start_min = times.min()
        latest_start = times.max() - pd.Timedelta(hours=hours)

        if latest_start < start_min:
            continue

        span_seconds = int((latest_start - start_min).total_seconds())
        offset = int(rng.integers(0, span_seconds + 1)) if span_seconds else 0
        start = start_min + pd.Timedelta(seconds=offset)
        end = start + pd.Timedelta(hours=hours)

        # Half-open interval [start, end); preserve original timestamps.
        block = group[(times >= start) & (times < end)]
        chosen_ids.extend(block.index.tolist())

    results.append(save_scenario(
        f"block_{hours}h",
        valid.loc[chosen_ids],
        seed,
        f"One random {hours}-hour outage per station; elapsed-time interval"
    ))

# 3. Whole-sensor failures: randomly select 10%, 20%, or 30% of
# eligible CPCB sensors. Each scenario is reproducible.
sensor_ids = sorted(valid["station_id"].unique().tolist())

for pct in [10, 20, 30]:
    seed = SEED + 2000 + pct
    rng = np.random.default_rng(seed)
    n_sensors = max(1, int(round(len(sensor_ids) * pct / 100)))
    failed = sorted(rng.choice(sensor_ids, size=n_sensors, replace=False).tolist())
    selected = valid[valid["station_id"].isin(failed)]

    results.append(save_scenario(
        f"sensor_failure_{pct}pct",
        selected,
        seed,
        f"All eligible observations hidden for {n_sensors} sensors: {failed}"
    ))

summary = pd.DataFrame(results)
summary.to_csv(MASK_DIR / "mask_manifest.csv", index=False)

metadata = {
    "base_seed": SEED,
    "input_file": str(INPUT),
    "input_rows": int(len(df)),
    "eligible_pm25_rows": int(len(valid)),
    "eligible_cpcb_sensors": int(valid["station_id"].nunique()),
    "random_percentages": [10, 30, 50],
    "block_hours": [6, 12, 24, 48],
    "whole_sensor_failure_percentages": [10, 20, 30],
    "ground_truth_separate": True,
    "original_dataset_modified": False,
}
(MASK_DIR / "mask_metadata.json").write_text(json.dumps(metadata, indent=2))

print("=== MASK GENERATION COMPLETE ===")
print(summary.to_string(index=False))
print("\nEligible readings:", len(valid))
print("CPCB sensors:", valid["station_id"].nunique())
print("Masks:", MASK_DIR)
print("Ground truth:", TRUTH_DIR)
print("Manifest:", MASK_DIR / "mask_manifest.csv")
