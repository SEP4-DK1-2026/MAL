# Orchestrate the full ML pipeline.

from pipeline.preprocessing import prepare_data
from pipeline.feature_engineering import prepare_features_and_targets
from pipeline.data_splitting import prepare_data_splits
from pipeline.model import build_model
from pipeline.training import train_model
from pipeline.evaluation import evaluate_model, plot_training_history, plot_predictions_vs_actual
import matplotlib.pyplot as plt


def run_pipeline():
    # Execute all pipeline steps in sequence.
    print("=" * 60)
    print("Step 1: Data Preprocessing")
    print("=" * 60)
    df = prepare_data()
    print(f"Loaded data: {df.shape[0]} rows, {df.shape[1]} columns")
    print("Data sample:")
    print(df.head())
    
    print("\n" + "=" * 60)
    print("Step 2: Feature Engineering")
    print("=" * 60)
    X, y, horizon_stats, future_temps = prepare_features_and_targets(df)
    print(f"Features (X): {X.shape}")
    print(f"Targets (y): {y.shape}")
    print("\nFeature columns:")
    print(X.columns.tolist())
    
    # Print per-horizon row counts
    print("\nRows per horizon (before/after cap):")
    for h, before, after in horizon_stats:
        status = "✓" if before == after else f"→ {after}"
        print(f"  {h:2d}h: {before:6d} {status}")
    
    sample = X.head(10).copy()
    sample['delta_temp'] = y.head(10).values
    sample = sample[['temp_dry', 'forecast_horizon_hours', 'delta_temp', 'humidity', 'pressure', 'wind_speed']]
    print("\nVerification sample:")
    print(sample)
    
    # Detailed row verification
    print("\n" + "=" * 60)
    print("Detailed Verification (2 sample rows)")
    print("=" * 60)
    
    for sample_idx in [0, min(500, len(X) - 1)]:
        if sample_idx < len(X):
            current_temp = X.iloc[sample_idx]['temp_dry']
            horizon_h = X.iloc[sample_idx]['forecast_horizon_hours']
            actual_future_temp = future_temps.iloc[sample_idx]
            delta = y.iloc[sample_idx]
            
            print(f"\nSample row {sample_idx}:")
            print(f"  Current temp: {current_temp:.1f}°C")
            print(f"  Horizon: {horizon_h:.0f}h")
            print(f"  Future temp (observed): {actual_future_temp:.1f}°C")
            print(f"  Delta temp: {delta:.1f}°C (future - current)")
    
    print("\n" + "=" * 60)
    print("Step 3: Data Splitting")
    print("=" * 60)
    splits = prepare_data_splits(X, y)
    X_train = splits['X_train']
    X_val = splits['X_val']
    X_test = splits['X_test']
    y_train = splits['y_train']
    y_val = splits['y_val']
    y_test = splits['y_test']
    scaler = splits['scaler']
    
    print(f"Training set: {X_train.shape[0]} samples (70% oldest data)")
    print(f"Validation set: {X_val.shape[0]} samples (15% middle data)")
    print(f"Test set: {X_test.shape[0]} samples (15% most recent data)")
    print(f"Total: {X_train.shape[0] + X_val.shape[0] + X_test.shape[0]} samples")
    
    # Verify horizon distribution across splits
    print("\nHorizon distribution check (all 72 horizons in each split):")
    for split_name, X_split in [('Train', X_train), ('Val', X_val), ('Test', X_test)]:
        horizons_in_split = sorted(X_split['forecast_horizon_hours'].unique())
        n_horizons = len(horizons_in_split)
        has_all = n_horizons == 72
        status = "✓ All 72 horizons" if has_all else f"⚠️  {n_horizons}/72 horizons"
        print(f"  {split_name:12} {status}")
    
    print("\n✅ TEMPORAL SPLIT (Sorted by Unix Timestamp):")
    print(f"  Sorted entire dataset by observed_unix (ascending)")
    print(f"  Training: 70% oldest observations")
    print(f"  Validation: 15% middle observations")
    print(f"  Test: 15% most recent observations")
    print(f"\n  Benefits:")
    print(f"  • All horizons distributed across each split")
    print(f"  • No data leakage - model never saw validation/test data during training")
    print(f"  • Realistic evaluation - true future prediction")
    
    print("\n" + "=" * 60)
    print("Step 4: Model Training")
    print("=" * 60)
    
    # Build model with single output (delta_temp prediction)
    input_dim = X_train.shape[1]  # Number of features
    output_dim = 1  # Single target: delta_temp
    model = build_model(input_dim, output_dim)
    
    print(f"Model built: {input_dim} features -> {output_dim} output (delta_temp)")
    print("\nModel architecture:")
    model.summary()
    
    # Train model with early stopping
    print("\nTraining model...")
    history = train_model(model, X_train.values, y_train.values, X_val.values, y_val.values)
    
    print("\n" + "=" * 60)
    print("Training Complete")
    print("=" * 60)
    print(f"Epochs trained: {len(history.history['loss'])}")
    print(f"Final training loss: {history.history['loss'][-1]:.4f}")
    print(f"Final validation loss: {history.history['val_loss'][-1]:.4f}")
    print(f"Final training MAE: {history.history['mae'][-1]:.4f}°C")
    print(f"Final validation MAE: {history.history['val_mae'][-1]:.4f}°C")
    
    # Check for early stopping
    best_val_loss = min(history.history['val_loss'])
    best_epoch = history.history['val_loss'].index(best_val_loss) + 1
    print(f"Best validation loss: {best_val_loss:.4f} (at epoch {best_epoch})")
    
    print("\n" + "=" * 60)
    print("Step 5: Model Evaluation")
    print("=" * 60)
    
    # Evaluate on test set
    y_pred_test, metrics_test = evaluate_model(model, X_test.values, y_test.values)
    print(f"\nTest Set Performance:")
    print(f"  MAE (Mean Absolute Error): {metrics_test['mae']:.4f}°C")
    print(f"  RMSE (Root Mean Squared Error): {metrics_test['rmse']:.4f}°C")
    print(f"  R² Score: {metrics_test['r2']:.4f}")
    
    # Evaluate on validation set for comparison
    y_pred_val, metrics_val = evaluate_model(model, X_val.values, y_val.values)
    print(f"\nValidation Set Performance (for comparison):")
    print(f"  MAE: {metrics_val['mae']:.4f}°C")
    print(f"  RMSE: {metrics_val['rmse']:.4f}°C")
    print(f"  R² Score: {metrics_val['r2']:.4f}")
    
    # Generate visualizations
    print("\nGenerating evaluation plots...")
    fig1 = plot_training_history(history)
    fig1.savefig('models/training_history.png', dpi=150, bbox_inches='tight')
    print("  ✓ Training history saved to models/training_history.png")
    
    fig2 = plot_predictions_vs_actual(y_test, y_pred_test, 'Test Set: Predicted vs Actual Temperature Change')
    fig2.savefig('models/predictions_vs_actual.png', dpi=150, bbox_inches='tight')
    print("  ✓ Predictions plot saved to models/predictions_vs_actual.png")
    
    plt.show()
    
    print("\n" + "=" * 60)
    print("Pipeline Complete!")
    print("=" * 60)
    print("\nModel saved to: models/model.keras")
    print("Evaluation plots saved to: models/*.png")
    print("\nNext: Use forecast_visualization.ipynb to test predictions on new dates")
