"""Step 2: Feature engineering (encodings + horizon-conditioned samples)."""

import numpy as np
import pandas as pd
from pipeline.config import TARGET_COLUMN, HORIZONS, SEED

# Progress bar
try:
    from tqdm import tqdm
except Exception:
    tqdm = None


def add_wind_dir_encoding(df):
    # Add circular encodings for wind direction: `wind_dir_sin`, `wind_dir_cos`.
    df = df.copy()
    df['wind_dir_sin'] = np.sin(np.radians(df['wind_dir']))
    df['wind_dir_cos'] = np.cos(np.radians(df['wind_dir']))
    return df


def add_hour_of_day_encoding(df):
    # Add hour-of-day circular encodings: `hour`, `hour_sin`, `hour_cos`.
    df = df.copy()
    df['hour'] = pd.to_datetime(df['observed']).dt.hour
    df['hour_sin'] = np.sin(2 * np.pi * df['hour'] / 24)
    df['hour_cos'] = np.cos(2 * np.pi * df['hour'] / 24)
    return df


def add_day_of_year_encoding(df):
    # Add day-of-year circular encodings: `doy`, `doy_sin`, `doy_cos`.
    df = df.copy()
    df['doy'] = pd.to_datetime(df['observed']).dt.dayofyear
    df['doy_sin'] = np.sin(2 * np.pi * df['doy'] / 365.25)
    df['doy_cos'] = np.cos(2 * np.pi * df['doy'] / 365.25)
    return df


def _add_circular_encodings(df):
    # Compatibility wrapper that applies wind, hour, and day-of-year encodings.
    df = add_wind_dir_encoding(df)
    df = add_hour_of_day_encoding(df)
    df = add_day_of_year_encoding(df)
    return df


def select_features(df, include_horizon=True):
    # Select core features for temperature forecasting.
    # - temp_dry: current temperature (for context and delta computation)
    # - humidity, pressure, wind_speed: Direct atmospheric factors
    # - wind_dir_sin, wind_dir_cos: Wind direction encoded as circular
    # - hour_sin, hour_cos: Daily cycle
    # - doy_sin, doy_cos: Seasonal cycle
    feature_cols = [
        'temp_dry', 'humidity', 'pressure', 'wind_speed',
        'wind_dir_sin', 'wind_dir_cos', 'hour_sin', 'hour_cos', 'doy_sin', 'doy_cos',
    ]
    if include_horizon:
        feature_cols.append('forecast_horizon_hours')
    return df[feature_cols].copy()


def prepare_features_and_targets(df, horizons=HORIZONS, recent_cutoff_seconds=604800, max_rows_per_horizon=50000):
    # Build horizon-conditioned dataset with delta_temp targets.
    # Filter out the most recent `recent_cutoff_seconds` to ensure future observations exist.
    # For each row and horizon, find the exact future observation (using unix second matching).
    # If no exact match exists, the row is discarded for that horizon.
    # If max_rows_per_horizon is set, randomly sample that many valid rows per horizon.
    
    df = _add_circular_encodings(df)
    
    # Filter out recent data (no future observations available for these rows)
    max_unix = df['observed_unix'].max()
    cutoff_unix = max_unix - recent_cutoff_seconds
    df_with_futures = df[df['observed_unix'] <= cutoff_unix].copy()
    
    # Sort by time for searchsorted (ascending: oldest to newest)
    df_sorted = df_with_futures.sort_values('observed_unix').reset_index(drop=True)
    times_sorted = df_sorted['observed_unix'].values.astype('int64')
    temps_sorted = df_sorted[TARGET_COLUMN].values.astype(float)
    
    rng = np.random.default_rng(SEED)
    X_parts = []
    y_parts = []
    future_temps_parts = []  # Track actual future temperatures
    horizons_iter = (tqdm(horizons, desc='horizons') if tqdm is not None else horizons)
    horizon_stats = []  # Track rows per horizon
    
    # Extract features from SORTED dataframe to match delta_series order
    base_features = select_features(df_sorted, include_horizon=False)
    
    for horizon_hours in horizons_iter:
        # Compute target times for each row (exact match: current_unix + horizon_hours*3600)
        target_times = times_sorted + int(horizon_hours * 3600)
        
        # Find exact match position for each target time using searchsorted
        idxs = np.searchsorted(times_sorted, target_times, side='left')
        
        # Vectorized check for exact match
        valid_match = np.zeros(len(times_sorted), dtype=bool)
        valid_match[idxs < len(times_sorted)] = (times_sorted[idxs[idxs < len(times_sorted)]] == target_times[idxs < len(times_sorted)])
        
        # Compute delta temps and capture actual future temps
        delta_temps = np.full(len(times_sorted), np.nan, dtype=float)
        future_temps_full = np.full(len(times_sorted), np.nan, dtype=float)
        delta_temps[valid_match] = temps_sorted[idxs[valid_match]] - temps_sorted[valid_match]
        future_temps_full[valid_match] = temps_sorted[idxs[valid_match]]
        
        # Create series indexed to sorted dataframe
        delta_series = pd.Series(delta_temps, index=df_sorted.index)
        future_temps_series = pd.Series(future_temps_full, index=df_sorted.index)

        
        # Use all valid rows, optionally sample if limit is set
        valid_idx = delta_series.dropna().index
        n_valid_before = len(valid_idx)
        
        if max_rows_per_horizon and len(valid_idx) > max_rows_per_horizon:
            valid_idx = pd.Index(rng.choice(valid_idx.values, size=max_rows_per_horizon, replace=False))
        
        n_valid_after = len(valid_idx)
        horizon_stats.append((horizon_hours, n_valid_before, n_valid_after))
        
        X_h = base_features.iloc[valid_idx].copy()
        X_h['forecast_horizon_hours'] = float(horizon_hours)
        X_h['observed_unix'] = df_sorted['observed_unix'].iloc[valid_idx].values
        y_h = delta_series.iloc[valid_idx].rename('delta_temp')
        future_temps_h = future_temps_series.iloc[valid_idx].reset_index(drop=True)
        
        X_parts.append(X_h.reset_index(drop=True))
        y_parts.append(y_h.reset_index(drop=True))
        future_temps_parts.append(future_temps_h)

    
    if X_parts:
        X = pd.concat(X_parts, axis=0).reset_index(drop=True)
        y = pd.concat(y_parts, axis=0).reset_index(drop=True)
        future_temps = pd.concat(future_temps_parts, axis=0).reset_index(drop=True)
    else:
        X = pd.DataFrame()
        y = pd.Series(dtype=float)
        future_temps = pd.Series(dtype=float)
        horizon_stats = []
    
    return X, y, horizon_stats, future_temps

