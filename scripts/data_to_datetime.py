import pandas as pd
from datetime import datetime
from pathlib import Path


def str_to_datetime(row):
    return datetime.strptime(row["observed"], "%Y-%m-%d %H:%M:%S").timestamp()


def transform_date_to_datetime(X):
    X_new = X.copy()
    X_new["observed"] = X.apply(str_to_datetime, axis="columns")
    return X_new


data = pd.read_csv(Path("./data/observations_06102.csv"))
data = transform_date_to_datetime(data)

data.to_csv()
with open(Path("./data/observations_06102_unixtime.csv"), "w", newline="\n") as f:
    data.to_csv(f, index=False)
