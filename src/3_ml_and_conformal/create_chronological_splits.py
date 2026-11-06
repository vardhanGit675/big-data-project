
from pathlib import Path
import json
import numpy as np
import pandas as pd

ROOT = Path(".")
SOURCE = ROOT / "final_datasets/sensors_data_2015-03-20_to_2026-03-27.csv"
MASK_DIR = ROOT / "data/masks"
TRUTH_DIR = ROOT / "data/ground_truth"
OUT = ROOT / "data/splits"

OUT.mkdir(parents=True, exist_ok=True)

# Preserve the same row IDs used when generating the masks.
df = pd.read_csv(SOURCE)
df["row_id"] = np.arange(len(df), dtype=np.int64)

df["collected_at"] = pd.to_datetime(
    df["collected_at"], errors="coerce", utc=True
)
df["value"] = pd.to_numeric(df["value"], errors="coerce")

eligible = (
    df["source"].eq("cpcb_caaqm")
    & df["qc_eligible"].astype(str).str.lower().isin(["true", "1"])
    & df["collected_at"].notna()
    & df["value"].notna()
    & np.isfinite(df["value"])
    & df["value"].ge(0)
    & df["value"].lt(500)
)

df = df.loc[eligible].copy()
df = df.sort_values(["collected_at", "row_id"]).reset_index(drop=True)

# Use unique timestamp boundaries so one timestamp cannot enter two splits.
timestamps = np.sort(df["collected_at"].unique())
if len(timestamps) < 3:
    raise ValueError("Not enough distinct timestamps to create splits.")

train_cut = timestamps[int(len(timestamps) * 0.70)]
val_cut = timestamps[int(len(timestamps) * 0.85)]

df["split"] = np.select(
    [
        df["collected_at"] < train_cut,
        df["collected_at"] < val_cut,
    ],
    ["train", "validation"],
    default="test",
)

# Save each split with original values and stable row IDs.
for name in ["train", "validation", "test"]:
    part = df.loc[df["split"] == name].copy()
    part.to_csv(OUT / f"{name}.csv", index=False)
    print(
        f"{name}: rows={len(part):,}, "
        f"from={part['collected_at'].min()}, "
        f"to={part['collected_at'].max()}"
    )

# Save row-ID mapping to keep masks and truth aligned with each split.
mapping = df[["row_id", "station_id", "collected_at", "split"]]
mapping.to_csv(OUT / "split_mapping.csv", index=False)

mask_files = sorted(MASK_DIR.glob("*.csv"))
mask_files = [p for p in mask_files if p.name != "mask_manifest.csv"]

for mask_path in mask_files:
    if "row_id" not in pd.read_csv(mask_path, nrows=0).columns:
        continue

    masks = pd.read_csv(mask_path)
    if "scenario" not in masks.columns:
        continue

    scenario = masks["scenario"].iloc[0]
    truth_path = TRUTH_DIR / f"{scenario}_truth.csv"

    if not truth_path.exists():
        raise FileNotFoundError(f"Missing ground truth: {truth_path}")

    truth = pd.read_csv(truth_path)

    # Ensure every masked ID has exactly one matching truth value.
    if masks["row_id"].duplicated().any() or truth["row_id"].duplicated().any():
        raise ValueError(f"Duplicate row IDs in scenario {scenario}")

    if set(masks["row_id"]) != set(truth["row_id"]):
        raise ValueError(f"Mask/truth row IDs differ for {scenario}")

    # Only retain masks whose observations belong to the eligible split data.
    split_masks = masks.merge(
        mapping[["row_id", "split"]],
        on="row_id",
        how="inner",
        validate="one_to_one",
    )

    split_truth = truth.merge(
        split_masks[["row_id", "split"]],
        on="row_id",
        how="inner",
        validate="one_to_one",
    )

    for name in ["train", "validation", "test"]:
        split_masks.loc[split_masks["split"] == name].drop(
            columns="split"
        ).to_csv(
            OUT / f"{scenario}_{name}_mask.csv", index=False
        )

        split_truth.loc[split_truth["split"] == name].drop(
            columns="split"
        ).to_csv(
            OUT / f"{scenario}_{name}_truth.csv", index=False
        )

manifest = {
    "source": str(SOURCE),
    "eligible_rows": int(len(df)),
    "train_cutoff_utc": str(train_cut),
    "validation_cutoff_utc": str(val_cut),
    "split_counts": {
        name: int((df["split"] == name).sum())
        for name in ["train", "validation", "test"]
    },
    "method": "Chronological split using unique timestamp boundaries",
    "ratios_target": {"train": 0.70, "validation": 0.15, "test": 0.15},
}
(OUT / "split_metadata.json").write_text(json.dumps(manifest, indent=2))

print("\nSplit mapping and scenario-specific masks/truth saved to:", OUT)
print("Chronological splits created successfully.")

