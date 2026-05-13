from sklearn.model_selection import train_test_split
from datetime import datetime
import numpy as np


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
