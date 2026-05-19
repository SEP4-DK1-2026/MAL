# Step 5: Model evaluation and metrics.

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.metrics import r2_score, mean_absolute_error, mean_squared_error
from pipeline.config import HORIZONS


def evaluate_model(model, X_test, y_test):
    # Evaluate model on test set and compute regression metrics.
    # Returns predictions and metrics (MAE, MSE, RMSE, R²).
    
    # Get predictions
    y_pred = model.predict(X_test, verbose=0)
    y_pred_flat = y_pred.flatten()
    
    # Convert to numpy if Series
    if isinstance(y_test, pd.Series):
        y_test_flat = y_test.values
    else:
        y_test_flat = np.array(y_test).flatten()
    
    # Compute metrics
    mae = mean_absolute_error(y_test_flat, y_pred_flat)
    mse = mean_squared_error(y_test_flat, y_pred_flat)
    rmse = np.sqrt(mse)
    r2 = r2_score(y_test_flat, y_pred_flat)
    
    metrics = {
        'mae': mae,
        'mse': mse,
        'rmse': rmse,
        'r2': r2
    }
    
    return y_pred_flat, metrics


def plot_training_history(history):
    # Plot training and validation loss curves from training history.
    # Shows loss and MAE for both training and validation sets.
    
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    
    # Loss plot
    ax1 = axes[0]
    ax1.plot(history.history['loss'], label='Training Loss', linewidth=2)
    ax1.plot(history.history['val_loss'], label='Validation Loss', linewidth=2)
    ax1.set_xlabel('Epoch', fontsize=12, fontweight='bold')
    ax1.set_ylabel('Loss (MSE)', fontsize=12, fontweight='bold')
    ax1.set_title('Training & Validation Loss', fontsize=14, fontweight='bold')
    ax1.legend(fontsize=11)
    ax1.grid(True, alpha=0.3)
    
    # MAE plot
    ax2 = axes[1]
    ax2.plot(history.history['mae'], label='Training MAE', linewidth=2)
    ax2.plot(history.history['val_mae'], label='Validation MAE', linewidth=2)
    ax2.set_xlabel('Epoch', fontsize=12, fontweight='bold')
    ax2.set_ylabel('MAE (°C)', fontsize=12, fontweight='bold')
    ax2.set_title('Training & Validation MAE', fontsize=14, fontweight='bold')
    ax2.legend(fontsize=11)
    ax2.grid(True, alpha=0.3)
    
    plt.tight_layout()
    return fig


def plot_predictions_vs_actual(y_test, y_pred, title='Test Set: Predicted vs Actual'):
    # Plot predicted vs actual values with scatter and diagonal reference line.
    
    if isinstance(y_test, pd.Series):
        y_test_flat = y_test.values
    else:
        y_test_flat = np.array(y_test).flatten()
    
    y_pred_flat = np.array(y_pred).flatten()
    
    fig, ax = plt.subplots(figsize=(10, 8))
    
    # Scatter plot
    ax.scatter(y_test_flat, y_pred_flat, alpha=0.5, s=30)
    
    # Perfect prediction line
    min_val = min(y_test_flat.min(), y_pred_flat.min())
    max_val = max(y_test_flat.max(), y_pred_flat.max())
    ax.plot([min_val, max_val], [min_val, max_val], 'r--', linewidth=2, label='Perfect Prediction')
    
    ax.set_xlabel('Actual Temperature Change (°C)', fontsize=12, fontweight='bold')
    ax.set_ylabel('Predicted Temperature Change (°C)', fontsize=12, fontweight='bold')
    ax.set_title(title, fontsize=14, fontweight='bold')
    ax.legend(fontsize=11)
    ax.grid(True, alpha=0.3)
    
    return fig

