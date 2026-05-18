from sklearn.model_selection import train_test_split
from datetime import datetime
import numpy as np
import pandas as pd


def _encode_cyclic(value, range_start, range_stop):
    value = (value - range_start) / (range_stop - range_start)
    x = np.sin(2 * np.pi * value)
    y = np.cos(2 * np.pi * value)
    return x, y


def encode_cyclic(
    X,
    feature,
    range_start,
    range_stop,
    new_feature_suffix=None,
    remove_old=False,
    get_feature=None,
):
    if get_feature is None:

        def get_feature(row):
            return row[feature]

    if new_feature_suffix is None:
        new_feature_suffix = feature
    X[f"{new_feature_suffix}_sin"] = X[[feature]].apply(
        lambda row: _encode_cyclic(get_feature(row), range_start, range_stop)[0],
        axis="columns",
    )
    X[f"{new_feature_suffix}_cos"] = X[[feature]].apply(
        lambda row: _encode_cyclic(get_feature(row), range_start, range_stop)[1],
        axis="columns",
    )
    if remove_old:
        X.drop([feature], inplace=True)
    return X


def split_time(
    X,
    year=True,
    month=True,
    day=True,
    hour=True,
    minute=False,
    second=False,
    make_cyclic=False,
    remove_old=True,
):
    X = X.copy()
    if year:
        X["year"] = X[["time"]].apply(
            lambda row: datetime.fromtimestamp(row["time"]).year, axis="columns"
        )
    if month:
        if not make_cyclic:
            X["month"] = X[["time"]].apply(
                lambda row: datetime.fromtimestamp(row["time"]).month, axis="columns"
            )
        else:
            encode_cyclic(
                X,
                "time",
                1,
                12,
                new_feature_suffix="month",
                get_feature=lambda row: datetime.fromtimestamp(row["time"]).month,
            )
    if day:
        if not make_cyclic:
            X["day"] = X[["time"]].apply(
                lambda row: datetime.fromtimestamp(row["time"]).day, axis="columns"
            )
        else:
            encode_cyclic(
                X,
                "time",
                1,
                31,
                new_feature_suffix="day",
                get_feature=lambda row: datetime.fromtimestamp(row["time"]).day,
            )
    if hour:
        if not make_cyclic:
            X["hour"] = X[["time"]].apply(
                lambda row: datetime.fromtimestamp(row["time"]).hour, axis="columns"
            )
        else:
            encode_cyclic(
                X,
                "time",
                0,
                23,
                new_feature_suffix="hour",
                get_feature=lambda row: datetime.fromtimestamp(row["time"]).hour,
            )
    if minute:
        if not make_cyclic:
            X["minute"] = X[["time"]].apply(
                lambda row: datetime.fromtimestamp(row["time"]).minute, axis="columns"
            )
        else:
            encode_cyclic(
                X,
                "time",
                0,
                59,
                new_feature_suffix="minute",
                get_feature=lambda row: datetime.fromtimestamp(row["time"]).minute,
            )
    if second:
        if not make_cyclic:
            X["second"] = X[["time"]].apply(
                lambda row: datetime.fromtimestamp(row["time"]).second, axis="columns"
            )
        else:
            encode_cyclic(
                X,
                "time",
                0,
                59,
                new_feature_suffix="second",
                get_feature=lambda row: datetime.fromtimestamp(row["time"]).second,
            )

    if remove_old:
        X.drop(["time"], axis="columns", inplace=True)

    return X


def split_set(X, y, seed, include_validate=True):
    X_tune, X_test, y_tune, y_test = train_test_split(
        X, y, test_size=0.2, random_state=seed
    )
    if not include_validate:
        return ((X_tune, y_tune), (X_test, y_test))
    X_train, X_validate, y_train, y_validate = train_test_split(
        X_tune, y_tune, test_size=0.2, random_state=seed
    )
    return ((X_train, y_train), (X_validate, y_validate), (X_test, y_test))


def get_sets(data, seed=42, do_split=True, drop_y_nan=True, include_validate=True):
    y_labels = [label for label in data.columns if label.startswith("future_")]

    if drop_y_nan:
        data = data.dropna(subset=y_labels)
    X = data.drop([*y_labels], axis="columns")
    y = data[y_labels]

    if do_split:
        return split_set(X, y, seed, include_validate=include_validate)
    else:
        return (X, y)


def drop_before(X, before_date):
    return X.drop(X[X["time"] < before_date.timestamp()].index)


def get_closest_row(data, timestamp, cache, strict=True):
    closest_row = None
    if timestamp not in cache:
        abs_difference = abs(data["time"] - timestamp)
        closest_row_idx = abs_difference.idxmin()
        if strict and abs_difference[closest_row_idx] > 5 * 60:
            closest_row = None
            closest_row_idx = None
        else:
            closest_row = data.iloc[closest_row_idx]
        cache[timestamp] = closest_row_idx
    else:
        closest_row_idx = cache[timestamp]
        if closest_row_idx is not None:
            closest_row = data.iloc[closest_row_idx]
    return closest_row


def add_target(data, features, days_range=7):
    new_rows = []
    hours = days_range * 24 + 1
    for row in data.iloc:
        new_rows.extend([row] * hours)
    new_data = pd.DataFrame(new_rows, columns=data.columns)

    predictions_offsets = range(0, hours)
    new_data["prediction_offset"] = [*predictions_offsets] * len(data)

    print("Done preparing for target")

    closets_row_cache = {}
    new_cols = {}
    for row in new_data.iloc:
        future_row = get_closest_row(
            data, row["time"] + row["prediction_offset"] * 60 * 60, closets_row_cache
        )

        for feature in features:
            new_cols.setdefault(f"future_{feature}", [])
            new_cols[f"future_{feature}"].append(
                future_row[feature] if future_row is not None else None
            )

    for key, value in new_cols.items():
        new_data[key] = value

    return new_data
