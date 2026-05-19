# Step 3: Data splitting and scaling.

import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler
from pipeline.config import SEED, TRAIN_RATIO, VAL_RATIO, TEST_RATIO


def prepare_data_splits(X, y):
    # Split X, y into train/val/test sets using TEMPORAL split based on unix timestamps.
    # Critical for time series: training data must be strictly before validation,
    # which is strictly before test data. This prevents data leakage and ensures
    # the model is evaluated on truly unseen future data.
    
    # Sort by unix timestamp to ensure chronological order
    X = X.sort_values('observed_unix').reset_index(drop=True)
    y = y.loc[X.index].reset_index(drop=True)
    
    # Calculate split based on timestamps (70% oldest, 15% middle, 15% most recent)
    n_samples = len(X)
    train_end = int(n_samples * TRAIN_RATIO)
    val_end = int(n_samples * (TRAIN_RATIO + VAL_RATIO))
    
    # Temporal split: oldest data → train, middle → val, most recent → test
    X_train = X.iloc[:train_end].copy()
    X_val = X.iloc[train_end:val_end].copy()
    X_test = X.iloc[val_end:].copy()
    
    y_train = y.iloc[:train_end].copy()
    y_val = y.iloc[train_end:val_end].copy()
    y_test = y.iloc[val_end:].copy()
    
    # Drop observed_unix before scaling (used only for temporal ordering)
    X_train = X_train.drop(columns=['observed_unix'])
    X_val = X_val.drop(columns=['observed_unix'])
    X_test = X_test.drop(columns=['observed_unix'])
    
    # Fit scaler on training data only, then apply to all sets
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_val_scaled = scaler.transform(X_val)
    X_test_scaled = scaler.transform(X_test)
    
    # Return as DataFrames to preserve column names
    X_train_scaled = pd.DataFrame(X_train_scaled, columns=X_train.columns)
    X_val_scaled = pd.DataFrame(X_val_scaled, columns=X_val.columns)
    X_test_scaled = pd.DataFrame(X_test_scaled, columns=X_test.columns)
    
    return {
        'X_train': X_train_scaled,
        'X_val': X_val_scaled,
        'X_test': X_test_scaled,
        'y_train': y_train.reset_index(drop=True),
        'y_val': y_val.reset_index(drop=True),
        'y_test': y_test.reset_index(drop=True),
        'scaler': scaler
    }
