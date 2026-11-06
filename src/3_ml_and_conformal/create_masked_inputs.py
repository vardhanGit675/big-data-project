from pathlib import Path
import json
import pandas as pd

SPLIT_DIR = Path("data/splits")
OUTPUT_DIR = Path("data/masked_inputs")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

SPLITS = ["train", "validation", "test"]
SCENARIOS = [
    "random_10pct", "random_30pct", "random_50pct",
    "block_6h", "block_12h", "block_24h", "block_48h",
    "sensor_failure_10pct", "sensor_failure_20pct",
    "sensor_failure_30pct",
]

manifest = []

for scenario in SCENARIOS:
    for split in SPLITS:
        source_path = SPLIT_DIR / f"{split}.csv"
        mask_path = SPLIT_DIR / f"{scenario}_{split}_mask.csv"
        truth_path = SPLIT_DIR / f"{scenario}_{split}_truth.csv"

        if not mask_path.exists() or not truth_path.exists():
            print(f"SKIP {scenario}/{split}: mask or truth file absent")
            continue

        df = pd.read_csv(source_path)
        mask = pd.read_csv(mask_path)
        truth = pd.read_csv(truth_path)

        for label, frame in [("input", df), ("mask", mask), ("truth", truth)]:
            if "row_id" not in frame.columns:
                raise ValueError(f"{label} missing row_id: {scenario}/{split}")

        if df["row_id"].duplicated().any():
            raise ValueError(f"Duplicate input row_id: {split}")

        if mask["row_id"].duplicated().any() or truth["row_id"].duplicated().any():
            raise ValueError(f"Duplicate mask/truth IDs: {scenario}/{split}")

        if set(mask["row_id"]) != set(truth["row_id"]):
            raise ValueError(f"Mask/truth IDs differ: {scenario}/{split}")

        unknown = set(mask["row_id"]) - set(df["row_id"])
        if unknown:
            raise ValueError(
                f"{len(unknown)} masked IDs absent from {split} input"
            )

        if "ground_truth_pm25" not in truth.columns:
            raise ValueError(f"Missing ground_truth_pm25: {scenario}/{split}")

        # Verify the stored truth matches the original split values.
        original = df[["row_id", "value"]].merge(
            truth[["row_id", "ground_truth_pm25"]],
            on="row_id",
            how="inner",
            validate="one_to_one",
        )

        if len(original) != len(truth):
            raise ValueError(f"Truth rows failed to match input: {scenario}/{split}")

        if not (
            pd.to_numeric(original["value"], errors="coerce").to_numpy()
            == pd.to_numeric(original["ground_truth_pm25"], errors="coerce").to_numpy()
        ).all():
            raise ValueError(f"Truth values differ from source: {scenario}/{split}")

        # Keep a separate copy and hide only selected PM2.5 values.
        masked_df = df.copy()
        masked_ids = set(mask["row_id"])
        hit = masked_df["row_id"].isin(masked_ids)

        masked_df.loc[hit, "value"] = float("nan")

        if masked_df.loc[hit, "value"].notna().any():
            raise ValueError(f"Some masked values remain visible: {scenario}/{split}")

        out_path = OUTPUT_DIR / f"{scenario}_{split}_input.csv"
        masked_df.to_csv(out_path, index=False)

        manifest.append({
            "scenario": scenario,
            "split": split,
            "source_rows": len(df),
            "masked_rows": int(hit.sum()),
            "output_file": str(out_path),
            "truth_file": str(truth_path),
        })

        print(
            f"PASS {scenario}/{split}: "
            f"{int(hit.sum()):,} values hidden; "
            f"{len(df):,} rows preserved"
        )

pd.DataFrame(manifest).to_csv(OUTPUT_DIR / "manifest.csv", index=False)

metadata = {
    "method": "Copy each chronological split and replace selected value entries with NaN",
    "original_split_files_modified": False,
    "ground_truth_files_modified": False,
    "output_directory": str(OUTPUT_DIR),
    "generated_pairs": len(manifest),
}
(OUTPUT_DIR / "metadata.json").write_text(json.dumps(metadata, indent=2))

print("\nMasked-input generation complete.")
print("Generated pairs:", len(manifest))
print("Output:", OUTPUT_DIR)
