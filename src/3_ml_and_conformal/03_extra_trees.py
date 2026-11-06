
import os
import warnings
warnings.filterwarnings("ignore")
 
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns
import optuna
import joblib
 
from pathlib import Path
from scipy.interpolate import UnivariateSpline
 
from sklearn.base import BaseEstimator, RegressorMixin
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import (
    OneHotEncoder, SplineTransformer, StandardScaler, LabelEncoder
)
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.model_selection import GridSearchCV
from sklearn.inspection import permutation_importance
from sklearn.metrics import (
    mean_absolute_error,
    mean_squared_error,
    r2_score,
    accuracy_score,
    precision_recall_fscore_support,
    classification_report,
    confusion_matrix,
    roc_curve,
    auc
)
from sklearn.ensemble import ExtraTreesRegressor, ExtraTreesClassifier
 
optuna.logging.set_verbosity(optuna.logging.WARNING)
 
# =====================================================
# 1. CONFIGURATION
# =====================================================
 
MODEL_TAG = "extra_trees"
MODEL_LABEL = "Extra Trees"
 
BASE = Path(__file__).resolve().parent.parent
 
RESULTS = BASE / "results"
MODELS = BASE / "models"
GRAPHS = BASE / "graphs" / MODEL_TAG
OUT = RESULTS / MODEL_TAG
 
for folder in [RESULTS, MODELS, GRAPHS, OUT]:
    folder.mkdir(parents=True, exist_ok=True)
 
RANDOM_STATE = 42
 
BASELINE_ROWS = 150000
SEARCH_ROWS = 50000
FINAL_ROWS = 200000
VALIDATION_ROWS = 50000
 
OPTUNA_TRIALS = 10
 
# Set QUICK_TEST=1 to smoke-test the whole script in a minute
if os.environ.get("QUICK_TEST"):
    BASELINE_ROWS = SEARCH_ROWS = FINAL_ROWS = VALIDATION_ROWS = 3000
    OPTUNA_TRIALS = 2
 
N_CV_SPLITS = 5
ALPHA = 0.10                # 90% prediction intervals
FORECAST_HORIZONS = [1, 24]
 
TARGET = "value"
 
NUMERIC_FEATURES = [
    "temperature_2m",
    "relative_humidity_2m",
    "dew_point_2m",
    "latitude",
    "longitude",
    "hour",
    "day_of_week",
    "month",
    "day_of_year",
    "hour_sin",
    "hour_cos",
    "month_sin",
    "month_cos",
    "pm25_lag_1h",
    "pm25_lag_24h"
]
 
CATEGORICAL_FEATURES = ["station_id"]
 
FEATURES = NUMERIC_FEATURES + CATEGORICAL_FEATURES
 
# Forecasting also knows the PM2.5 reading at time t
FORECAST_NUMERIC = NUMERIC_FEATURES + ["pm25_now"]
FORECAST_FEATURES = FORECAST_NUMERIC + CATEGORICAL_FEATURES
 
# CPCB (India) PM2.5 AQI categories
AQI_NAMES = ["Good", "Satisfactory", "Moderate", "Poor", "Very Poor", "Severe"]
AQI_EDGES = [30, 60, 90, 120, 250]
CLASS_MIDPOINTS = np.array([15.0, 45.0, 75.0, 105.0, 185.0, 300.0])
 
 
def to_class(values):
    return np.digitize(np.asarray(values, dtype=float), AQI_EDGES, right=True)
 
 
# =====================================================
# MODEL DEFINITION - EXTRA TREES
# =====================================================
 
NEEDS_SCALING = False
IMPORTANCE_KIND = "tree"
 
DEFAULTS = {
    "n_estimators": 100,
    "max_depth": 20,
    "min_samples_split": 2,
    "min_samples_leaf": 2,
    "max_features": 1.0
}
 
 
def _params(params, parallel):
    p = {**DEFAULTS, **(params or {})}
    p["random_state"] = RANDOM_STATE
    p["n_jobs"] = -1 if parallel else 1
    return p
 
 
def make_regressor(params=None, parallel=False):
    return ExtraTreesRegressor(**_params(params, parallel))
 
 
def make_classifier(params=None, parallel=False):
    return ExtraTreesClassifier(**_params(params, parallel))
 
 
GRID_PARAMETERS = {
    "regressor__n_estimators": [80, 120],
    "regressor__max_depth": [15, 25],
    "regressor__min_samples_leaf": [2]
}
 
 
def suggest_params(trial):
    return {
        "n_estimators": trial.suggest_int("n_estimators", 80, 160, step=40),
        "max_depth": trial.suggest_int("max_depth", 12, 30),
        "min_samples_split": trial.suggest_int("min_samples_split", 2, 10),
        "min_samples_leaf": trial.suggest_int("min_samples_leaf", 1, 4),
        "max_features": trial.suggest_categorical(
            "max_features", ["sqrt", 0.8, 1.0]
        )
    }
 
 
# =====================================================
# 2. LOAD DATA
# =====================================================
 
print("\nLOADING DATA")
 
train = pd.read_csv(RESULTS / "train.csv")
val = pd.read_csv(RESULTS / "validation.csv")
 
train["collected_at"] = pd.to_datetime(train["collected_at"], utc=True)
val["collected_at"] = pd.to_datetime(val["collected_at"], utc=True)
 
train = train.dropna(subset=[TARGET])
val = val.dropna(subset=[TARGET])
 
print("Training rows:", len(train))
print("Validation rows:", len(val))
 
 
def sample_data(df, n):
    if len(df) > n:
        df = df.sample(n=n, random_state=RANDOM_STATE)
    return df.sort_values("collected_at").reset_index(drop=True)
 
 
baseline_data = sample_data(train, BASELINE_ROWS)
search_data = sample_data(train, SEARCH_ROWS)
final_data = sample_data(train, FINAL_ROWS)
validation = sample_data(val, VALIDATION_ROWS)
 
 
# =====================================================
# 3. PREPROCESSING + PIPELINES
# =====================================================
 
def build_preprocessor(numeric_features):
 
    numeric_steps = [
        ("imputer", SimpleImputer(strategy="median", add_indicator=True))
    ]
    if NEEDS_SCALING:
        numeric_steps.append(("scaler", StandardScaler()))
 
    categorical_pipeline = Pipeline([
        ("imputer", SimpleImputer(strategy="most_frequent")),
        ("encoder", OneHotEncoder(handle_unknown="ignore"))
    ])
 
    return ColumnTransformer([
        ("numeric", Pipeline(numeric_steps), numeric_features),
        ("categorical", categorical_pipeline, CATEGORICAL_FEATURES)
    ])
 
 
def create_pipeline(params=None, parallel=False, numeric_features=None):
    numeric_features = numeric_features or NUMERIC_FEATURES
    return Pipeline([
        ("preprocessor", build_preprocessor(numeric_features)),
        ("regressor", make_regressor(params, parallel))
    ])
 
 
def create_classifier_pipeline(params=None, parallel=False):
    return Pipeline([
        ("preprocessor", build_preprocessor(NUMERIC_FEATURES)),
        ("classifier", make_classifier(params, parallel))
    ])
 
 
def evaluate(y_true, predictions):
    # MAE / RMSE / R2 on PM2.5, plus Precision / Recall / F1 after
    # converting both actual and predicted PM2.5 into AQI categories.
    y_true = np.asarray(y_true, dtype=float)
    predictions = np.asarray(predictions, dtype=float)
 
    precision, recall, f1, _ = precision_recall_fscore_support(
        to_class(y_true),
        to_class(predictions),
        average="weighted",
        zero_division=0
    )
 
    return {
        "MAE": mean_absolute_error(y_true, predictions),
        "RMSE": np.sqrt(mean_squared_error(y_true, predictions)),
        "R2": r2_score(y_true, predictions),
        "Precision": precision,
        "Recall": recall,
        "F1": f1
    }
 
 
def conformal_interval(y_true, predictions, alpha=ALPHA):
    # Split-conformal: calibrate on the earlier half (by time),
    # measure coverage on the later half.
    y_true = np.asarray(y_true, dtype=float)
    predictions = np.asarray(predictions, dtype=float)
 
    half = len(y_true) // 2
    residuals = np.abs(y_true[:half] - predictions[:half])
 
    level = min(1.0, np.ceil((half + 1) * (1 - alpha)) / half)
    q = np.quantile(residuals, level)
 
    lower = np.maximum(predictions[half:] - q, 0)
    upper = predictions[half:] + q
 
    covered = (y_true[half:] >= lower) & (y_true[half:] <= upper)
 
    return {
        "half": half,
        "q": q,
        "lower": lower,
        "upper": upper,
        "coverage": float(covered.mean()),
        "width": float((upper - lower).mean())
    }
 
 
X_val = validation[FEATURES]
y_val = validation[TARGET]
 
 
# =====================================================
# 4. BASELINE
# =====================================================
 
print("\n" + "=" * 50)
print(MODEL_LABEL.upper() + " BASELINE")
print("=" * 50)
 
baseline = create_pipeline(parallel=True)
 
baseline.fit(baseline_data[FEATURES], baseline_data[TARGET])
 
baseline_predictions = baseline.predict(X_val)
baseline_metrics = evaluate(y_val, baseline_predictions)
 
print("Baseline results:")
print(baseline_metrics)
 
joblib.dump(baseline, MODELS / f"{MODEL_TAG}_baseline.pkl")
 
 
# =====================================================
# 5. 5-FOLD TIME-AWARE (EXPANDING WINDOW) CROSS VALIDATION
# =====================================================
 
def temporal_folds(data, n_splits=N_CV_SPLITS, first_start=0.50, window=0.10):
 
    timestamps = data["collected_at"].to_numpy()
    unique_times = np.unique(timestamps)
 
    folds = []
 
    for k in range(n_splits):
 
        start_fraction = first_start + k * window
        end_fraction = start_fraction + window
 
        start_index = int(len(unique_times) * start_fraction)
        end_index = min(int(len(unique_times) * end_fraction),
                        len(unique_times) - 1)
 
        start_time = unique_times[start_index]
        end_time = unique_times[end_index]
 
        train_indices = np.where(timestamps < start_time)[0]
 
        if k == n_splits - 1:
            val_indices = np.where(timestamps >= start_time)[0]
        else:
            val_indices = np.where(
                (timestamps >= start_time) & (timestamps < end_time)
            )[0]
 
        if len(train_indices) and len(val_indices):
            folds.append((train_indices, val_indices))
 
    return folds
 
 
X_search = search_data[FEATURES]
y_search = search_data[TARGET]
 
CV_SPLITS = temporal_folds(search_data)
 
print("\nTemporal CV folds:", len(CV_SPLITS))
 
if len(CV_SPLITS) < 2:
    raise ValueError("Not enough temporal data for CV.")
 
 
# =====================================================
# 6. GRID SEARCH CV
# =====================================================
 
print("\n" + "=" * 50)
print("GRID SEARCH OPTIMIZATION")
print("=" * 50)
 
grid = GridSearchCV(
    estimator=create_pipeline(),
    param_grid=GRID_PARAMETERS,
    scoring="neg_mean_absolute_error",
    cv=CV_SPLITS,
    n_jobs=-1,
    verbose=1,
    refit=False
)
 
grid.fit(X_search, y_search)
 
grid_params = {
    key.split("__")[-1]: value
    for key, value in grid.best_params_.items()
}
 
grid_mae = -grid.best_score_
 
print("Best parameters:", grid_params)
print("Best temporal CV MAE:", grid_mae)
 
print("\nEvaluating GridSearchCV model on validation data...")
 
grid_final_model = create_pipeline(grid_params, parallel=True)
 
grid_final_model.fit(final_data[FEATURES], final_data[TARGET])
 
grid_predictions = grid_final_model.predict(X_val)
grid_metrics = evaluate(y_val, grid_predictions)
 
print("GridSearchCV validation results:")
for metric, value in grid_metrics.items():
    print(f"{metric}: {value:.5f}")
 
joblib.dump(grid_final_model, MODELS / f"{MODEL_TAG}_gridsearch.pkl")
 
 
# =====================================================
# 7. OPTUNA OPTIMIZATION
# =====================================================
 
print("\n" + "=" * 50)
print("OPTUNA OPTIMIZATION")
print("=" * 50)
 
 
def objective(trial):
 
    params = suggest_params(trial)
    errors = []
 
    for train_idx, val_idx in CV_SPLITS:
 
        model = create_pipeline(params)
 
        model.fit(X_search.iloc[train_idx], y_search.iloc[train_idx])
 
        predictions = model.predict(X_search.iloc[val_idx])
 
        errors.append(
            mean_absolute_error(y_search.iloc[val_idx], predictions)
        )
 
    return np.mean(errors)
 
 
study = optuna.create_study(
    direction="minimize",
    sampler=optuna.samplers.TPESampler(seed=RANDOM_STATE)
)
 
study.optimize(objective, n_trials=OPTUNA_TRIALS)
 
optuna_params = study.best_params
 
print("Best Optuna parameters:", optuna_params)
print("Best Optuna CV MAE:", study.best_value)
 
 
# =====================================================
# 8. FINAL OPTIMIZED MODEL
# =====================================================
 
print("\n" + "=" * 50)
print("FINAL OPTIMIZED " + MODEL_LABEL.upper())
print("=" * 50)
 
final_model = create_pipeline(optuna_params, parallel=True)
 
final_model.fit(final_data[FEATURES], final_data[TARGET])
 
final_predictions = final_model.predict(X_val)
final_metrics = evaluate(y_val, final_predictions)
 
print("\nFINAL RESULTS")
for metric, value in final_metrics.items():
    print(f"{metric}: {value:.5f}")
 
joblib.dump(final_model, MODELS / f"{MODEL_TAG}_optimized.pkl")
 
 
# =====================================================
# 9. SAVE MODEL COMPARISON
# =====================================================
 
comparison = pd.DataFrame([
    {"Model": f"{MODEL_LABEL} Baseline", **baseline_metrics},
    {"Model": f"{MODEL_LABEL} GridSearchCV", **grid_metrics},
    {"Model": f"{MODEL_LABEL} Optuna", **final_metrics}
])
 
comparison.to_csv(OUT / f"{MODEL_TAG}_comparison.csv", index=False)
 
print("\nMODEL COMPARISON")
print(comparison.to_string(index=False))
 
 
# =====================================================
# 10. GRAPH 1 - ACTUAL VS PREDICTED
# =====================================================
 
plt.figure(figsize=(8, 6))
plt.scatter(y_val, final_predictions, alpha=0.25)
 
limits = [
    min(y_val.min(), final_predictions.min()),
    max(y_val.max(), final_predictions.max())
]
plt.plot(limits, limits, linestyle="--")
 
plt.xlabel("Actual PM2.5")
plt.ylabel("Predicted PM2.5")
plt.title(f"{MODEL_LABEL}: Actual vs Predicted")
plt.tight_layout()
plt.savefig(GRAPHS / "01_actual_vs_predicted.png", dpi=300)
plt.close()
 
 
# =====================================================
# 11. GRAPH 2 - RESIDUAL PLOT
# =====================================================
 
residuals = y_val.to_numpy() - final_predictions
 
plt.figure(figsize=(8, 6))
plt.scatter(final_predictions, residuals, alpha=0.25)
plt.axhline(0, linestyle="--")
 
plt.xlabel("Predicted PM2.5")
plt.ylabel("Residuals")
plt.title(f"{MODEL_LABEL} Residual Analysis")
plt.tight_layout()
plt.savefig(GRAPHS / "02_residual_plot.png", dpi=300)
plt.close()
 
 
# =====================================================
# 12. GRAPH 3 - FEATURE IMPORTANCE
# =====================================================
 
print("\nComputing feature importance...")
 
preprocessor = final_model.named_steps["preprocessor"]
 
if IMPORTANCE_KIND == "tree":
    feature_names = preprocessor.get_feature_names_out()
    importance = final_model.named_steps["regressor"].feature_importances_
 
elif IMPORTANCE_KIND == "linear":
    feature_names = preprocessor.get_feature_names_out()
    coef = final_model.named_steps["regressor"].clf_.coef_
    importance = np.abs(coef).mean(axis=0)
 
else:  # permutation importance (kernel models have no coefficients)
    perm_sample = X_val.sample(n=min(3000, len(X_val)),
                               random_state=RANDOM_STATE)
    perm = permutation_importance(
        final_model,
        perm_sample,
        y_val.loc[perm_sample.index],
        scoring="neg_mean_absolute_error",
        n_repeats=3,
        random_state=RANDOM_STATE,
        n_jobs=1
    )
    feature_names = np.array(FEATURES)
    importance = perm.importances_mean
 
importance_df = pd.DataFrame({
    "Feature": feature_names,
    "Importance": importance
}).sort_values("Importance", ascending=False)
 
importance_df.to_csv(OUT / f"{MODEL_TAG}_feature_importance.csv", index=False)
 
plt.figure(figsize=(10, 7))
sns.barplot(data=importance_df.head(15), x="Importance", y="Feature")
plt.title(f"{MODEL_LABEL} Feature Importance")
plt.tight_layout()
plt.savefig(GRAPHS / "03_feature_importance.png", dpi=300)
plt.close()
 
 
# =====================================================
# 13. GRAPH 4 - P-SPLINE
# =====================================================
 
print("\nGenerating P-spline graph...")
 
feature = "temperature_2m"
 
spline_data = sample_data(train, 20000)
 
x = spline_data[[feature]].copy()
y = spline_data[TARGET].to_numpy()
 
x[feature] = x[feature].fillna(x[feature].median())
 
spline_transformer = SplineTransformer(n_knots=8, degree=3, include_bias=True)
 
B = spline_transformer.fit_transform(x)
 
# Second-order difference penalty
D2 = np.diff(np.eye(B.shape[1]), n=2, axis=0)
penalty = 100 * (D2.T @ D2)
 
coefficients = np.linalg.solve(B.T @ B + penalty, B.T @ y)
 
x_grid = np.linspace(x[feature].min(), x[feature].max(), 300).reshape(-1, 1)
B_grid = spline_transformer.transform(pd.DataFrame(x_grid, columns=[feature]))
y_grid = B_grid @ coefficients
 
plt.figure(figsize=(9, 6))
plt.scatter(x[feature], y, alpha=0.08)
plt.plot(x_grid, y_grid, linewidth=3)
 
plt.xlabel("Temperature")
plt.ylabel("PM2.5")
plt.title("Penalized B-Spline (P-Spline)")
plt.tight_layout()
plt.savefig(GRAPHS / "04_p_spline.png", dpi=300)
plt.close()
 
 
# =====================================================
# 14. GRAPH 5 - SMOOTHING SPLINE
# =====================================================
 
print("\nGenerating smoothing spline graph...")
 
binned = spline_data.copy()
binned[feature] = binned[feature].fillna(binned[feature].median())
 
binned["bin"] = pd.qcut(binned[feature], q=100, duplicates="drop")
 
summary = binned.groupby("bin", observed=True).agg(
    x=(feature, "mean"),
    y=(TARGET, "mean"),
    count=(TARGET, "size")
).dropna().sort_values("x")
 
smooth_spline = UnivariateSpline(
    summary["x"],
    summary["y"],
    w=np.sqrt(summary["count"]),
    s=len(summary) * 10
)
 
smooth_x = np.linspace(summary["x"].min(), summary["x"].max(), 300)
 
plt.figure(figsize=(9, 6))
plt.scatter(summary["x"], summary["y"], alpha=0.5, label="Binned observations")
plt.plot(smooth_x, smooth_spline(smooth_x), linewidth=3,
         label="Smoothing spline")
 
plt.xlabel("Temperature")
plt.ylabel("Mean PM2.5")
plt.title("Smoothing Spline Analysis")
plt.legend()
plt.tight_layout()
plt.savefig(GRAPHS / "05_smoothing_spline.png", dpi=300)
plt.close()
 
 
# =====================================================
# 15. PREDICTION INTERVALS (90%, split-conformal)
# =====================================================
 
print("\nBuilding prediction intervals...")
 
pi = conformal_interval(y_val, final_predictions)
 
print(f"Interval half-width: {pi['q']:.3f}")
print(f"Coverage (target {(1 - ALPHA):.0%}): {pi['coverage']:.3f}")
print(f"Mean interval width: {pi['width']:.3f}")
 
test_part = validation.iloc[pi["half"]:].copy()
test_part["pred"] = final_predictions[pi["half"]:]
test_part["lower"] = pi["lower"]
test_part["upper"] = pi["upper"]
 
top_station = test_part["station_id"].value_counts().idxmax()
sub = test_part[test_part["station_id"] == top_station].head(300)
 
plt.figure(figsize=(11, 6))
plt.plot(range(len(sub)), sub[TARGET].to_numpy(), label="Actual", linewidth=1.5)
plt.plot(range(len(sub)), sub["pred"].to_numpy(), label="Predicted",
         linewidth=1.5)
plt.fill_between(range(len(sub)), sub["lower"].to_numpy(),
                 sub["upper"].to_numpy(), alpha=0.25,
                 label=f"{(1 - ALPHA):.0%} prediction interval")
 
plt.xlabel("Time step (single station)")
plt.ylabel("PM2.5")
plt.title(f"{MODEL_LABEL}: Prediction Intervals")
plt.legend()
plt.tight_layout()
plt.savefig(GRAPHS / "06_prediction_intervals.png", dpi=300)
plt.close()
 
 
# =====================================================
# 16. AQI CLASSIFICATION - CONFUSION MATRIX + ROC
# =====================================================
 
print("\n" + "=" * 50)
print("AQI CLASSIFICATION")
print("=" * 50)
 
y_train_class = to_class(final_data[TARGET].to_numpy())
y_val_class = to_class(y_val.to_numpy())
 
label_encoder = LabelEncoder().fit(y_train_class)
 
keep = np.isin(y_val_class, label_encoder.classes_)
 
X_val_cls = X_val[keep]
y_val_enc = label_encoder.transform(y_val_class[keep])
y_train_enc = label_encoder.transform(y_train_class)
 
class_names = [AQI_NAMES[c] for c in label_encoder.classes_]
n_classes = len(class_names)
 
classifier = create_classifier_pipeline(optuna_params, parallel=True)
 
classifier.fit(final_data[FEATURES], y_train_enc)
 
probabilities = classifier.predict_proba(X_val_cls)
class_predictions = probabilities.argmax(axis=1)
 
precision, recall, f1, _ = precision_recall_fscore_support(
    y_val_enc, class_predictions, average="weighted", zero_division=0
)
macro_p, macro_r, macro_f1, _ = precision_recall_fscore_support(
    y_val_enc, class_predictions, average="macro", zero_division=0
)
 
class_metrics = {
    "Accuracy": accuracy_score(y_val_enc, class_predictions),
    "Precision_weighted": precision,
    "Recall_weighted": recall,
    "F1_weighted": f1,
    "Precision_macro": macro_p,
    "Recall_macro": macro_r,
    "F1_macro": macro_f1
}
 
report = classification_report(
    y_val_enc, class_predictions,
    labels=list(range(n_classes)),
    target_names=class_names,
    output_dict=True,
    zero_division=0
)
pd.DataFrame(report).T.to_csv(
    OUT / f"{MODEL_TAG}_classification_report.csv"
)
 
joblib.dump(classifier, MODELS / f"{MODEL_TAG}_aqi_classifier.pkl")
 
# Confusion matrix (counts + row-normalised)
cm = confusion_matrix(y_val_enc, class_predictions,
                      labels=list(range(n_classes)))
cm_norm = cm / np.maximum(cm.sum(axis=1, keepdims=True), 1)
 
fig, axes = plt.subplots(1, 2, figsize=(15, 6))
 
sns.heatmap(cm, annot=True, fmt="d", cmap="Blues", cbar=False,
            xticklabels=class_names, yticklabels=class_names, ax=axes[0])
axes[0].set_title("Confusion Matrix (counts)")
axes[0].set_xlabel("Predicted AQI class")
axes[0].set_ylabel("Actual AQI class")
 
sns.heatmap(cm_norm, annot=True, fmt=".2f", cmap="Blues", cbar=False,
            xticklabels=class_names, yticklabels=class_names, ax=axes[1])
axes[1].set_title("Confusion Matrix (row-normalised)")
axes[1].set_xlabel("Predicted AQI class")
axes[1].set_ylabel("Actual AQI class")
 
plt.suptitle(f"{MODEL_LABEL}: AQI Classification")
plt.tight_layout()
plt.savefig(GRAPHS / "07_confusion_matrix.png", dpi=300)
plt.close()
 
# One-vs-rest ROC curves
onehot = np.eye(n_classes)[y_val_enc]
 
plt.figure(figsize=(8, 7))
 
aucs = []
for i in range(n_classes):
    positives = onehot[:, i].sum()
    if positives == 0 or positives == len(onehot):
        continue
    fpr, tpr, _ = roc_curve(onehot[:, i], probabilities[:, i])
    class_auc = auc(fpr, tpr)
    aucs.append(class_auc)
    plt.plot(fpr, tpr, linewidth=2,
             label=f"{class_names[i]} (AUC = {class_auc:.3f})")
 
plt.plot([0, 1], [0, 1], linestyle="--", color="grey")
plt.xlabel("False positive rate")
plt.ylabel("True positive rate")
plt.title(f"{MODEL_LABEL}: ROC Curves (one-vs-rest)")
plt.legend(loc="lower right")
plt.tight_layout()
plt.savefig(GRAPHS / "08_roc_curve.png", dpi=300)
plt.close()
 
class_metrics["ROC_AUC_macro"] = float(np.mean(aucs)) if aucs else np.nan
 
print("Classification metrics:")
for metric, value in class_metrics.items():
    print(f"{metric}: {value:.5f}")
 
pd.DataFrame([class_metrics]).to_csv(
    OUT / f"{MODEL_TAG}_classification_metrics.csv", index=False
)
 
 
# =====================================================
# 17. 1-HOUR AND 24-HOUR FORECASTING
# =====================================================
 
print("\n" + "=" * 50)
print("FORECASTING (1h and 24h ahead)")
print("=" * 50)
 
 
def add_future_target(df, hours):
    # target = PM2.5 at the same station `hours` later
    d = df.copy()
    d["hour_ts"] = d["collected_at"].dt.floor("h")
    d["pm25_now"] = d[TARGET]
    d = d.drop_duplicates(["station_id", "hour_ts"])
 
    future = d[["station_id", "hour_ts", TARGET]].copy()
    future["hour_ts"] = future["hour_ts"] - pd.Timedelta(hours=hours)
    future = future.rename(columns={TARGET: "target_h"})
 
    d = d.drop(columns=[TARGET]).merge(
        future, on=["station_id", "hour_ts"], how="inner"
    )
    return d
 
 
forecast_rows = []
 
fig, axes = plt.subplots(1, len(FORECAST_HORIZONS),
                         figsize=(7 * len(FORECAST_HORIZONS), 6))
axes = np.atleast_1d(axes)
 
for ax, hours in zip(axes, FORECAST_HORIZONS):
 
    f_train = add_future_target(train, hours)
    f_val = add_future_target(val, hours)
 
    if len(f_train) == 0 or len(f_val) == 0:
        print(f"[{hours}h] no matching future rows found - skipped")
        continue
 
    f_train = sample_data(f_train, FINAL_ROWS)
    f_val = sample_data(f_val, VALIDATION_ROWS)
 
    forecast_model = create_pipeline(
        optuna_params, parallel=True, numeric_features=FORECAST_NUMERIC
    )
 
    forecast_model.fit(f_train[FORECAST_FEATURES], f_train["target_h"])
 
    f_pred = forecast_model.predict(f_val[FORECAST_FEATURES])
    f_true = f_val["target_h"].to_numpy()
 
    model_metrics = evaluate(f_true, f_pred)
    persistence_metrics = evaluate(f_true, f_val["pm25_now"].to_numpy())
    f_pi = conformal_interval(f_true, f_pred)
 
    print(f"\n[{hours}h ahead] {MODEL_LABEL}: ", model_metrics)
    print(f"[{hours}h ahead] Persistence: ", persistence_metrics)
    print(f"[{hours}h ahead] 90% interval coverage: {f_pi['coverage']:.3f}, "
          f"width: {f_pi['width']:.2f}")
 
    forecast_rows.append({
        "Horizon_h": hours, "Model": MODEL_LABEL, **model_metrics,
        "PI_coverage": f_pi["coverage"], "PI_width": f_pi["width"]
    })
    forecast_rows.append({
        "Horizon_h": hours, "Model": "Persistence (PM2.5 now)",
        **persistence_metrics, "PI_coverage": np.nan, "PI_width": np.nan
    })
 
    joblib.dump(forecast_model, MODELS / f"{MODEL_TAG}_forecast_{hours}h.pkl")
 
    ax.scatter(f_true, f_pred, alpha=0.25)
    lim = [min(f_true.min(), f_pred.min()), max(f_true.max(), f_pred.max())]
    ax.plot(lim, lim, linestyle="--")
    ax.set_xlabel("Actual PM2.5")
    ax.set_ylabel("Predicted PM2.5")
    ax.set_title(f"{MODEL_LABEL}: {hours}h-ahead forecast "
                 f"(R2 = {model_metrics['R2']:.3f})")
 
plt.tight_layout()
plt.savefig(GRAPHS / "09_forecast_actual_vs_predicted.png", dpi=300)
plt.close()
 
forecast_df = pd.DataFrame(forecast_rows)
forecast_df.to_csv(OUT / f"{MODEL_TAG}_forecast_results.csv", index=False)
 
print("\nFORECAST SUMMARY")
print(forecast_df.to_string(index=False))
 
 
# =====================================================
# 18. SAVE FINAL EXPERIMENT RESULTS
# =====================================================
 
results = {
    "Baseline_MAE": baseline_metrics["MAE"],
    "Baseline_RMSE": baseline_metrics["RMSE"],
    "Baseline_R2": baseline_metrics["R2"],
    "Baseline_F1": baseline_metrics["F1"],
    "GridSearch_CV_MAE": grid_mae,
    "GridSearch_Validation_MAE": grid_metrics["MAE"],
    "GridSearch_Validation_RMSE": grid_metrics["RMSE"],
    "GridSearch_Validation_R2": grid_metrics["R2"],
    "GridSearch_Validation_F1": grid_metrics["F1"],
    "Optuna_CV_MAE": study.best_value,
    "Optuna_Validation_MAE": final_metrics["MAE"],
    "Optuna_Validation_RMSE": final_metrics["RMSE"],
    "Optuna_Validation_R2": final_metrics["R2"],
    "Optuna_Validation_Precision": final_metrics["Precision"],
    "Optuna_Validation_Recall": final_metrics["Recall"],
    "Optuna_Validation_F1": final_metrics["F1"],
    "PI_coverage": pi["coverage"],
    "PI_width": pi["width"],
    "AQI_Accuracy": class_metrics["Accuracy"],
    "AQI_F1_weighted": class_metrics["F1_weighted"],
    "AQI_ROC_AUC_macro": class_metrics["ROC_AUC_macro"]
}
 
pd.DataFrame([results]).to_csv(OUT / f"{MODEL_TAG}_final_results.csv",
                               index=False)
 
print("\n" + "=" * 50)
print(MODEL_LABEL.upper() + " EXPERIMENTS COMPLETED")
print("=" * 50)
 
print("\nModels saved in:", MODELS)
print("Graphs saved in:", GRAPHS)
print("Results saved in:", OUT)
 