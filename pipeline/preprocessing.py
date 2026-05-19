# Step 1: Data loading and preprocessing.

import pandas as pd
from pipeline.config import DATA_PATH, TARGET_COLUMN


def load_data():
    # Load CSV data.
    df = pd.read_csv(DATA_PATH, parse_dates=['observed'])
    return df


def clean_data(df):
    # Handle missing values:
    # - Treat precip_past1h blanks as 0 (no precipitation)
    # - Drop other NaN rows
    df = df.copy()
    df.fillna({'precip_past1h': 0.0}, inplace=True)
    df = df.dropna()
    return df


def prepare_data():
    # Load, clean, and return processed dataframe.
    df = load_data()
    df = clean_data(df)
    # Ensure observed is datetime and add unix seconds column for fast lookup
    df['observed'] = pd.to_datetime(df['observed'])
    df['observed_unix'] = (df['observed'].astype('int64') // 10**6).astype('int64')
    return df
