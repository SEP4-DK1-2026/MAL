# Step 4b: Training loop with early stopping.

import numpy as np
from tensorflow import keras
from pipeline.config import EPOCHS, BATCH_SIZE, EARLY_STOPPING_PATIENCE, EARLY_STOPPING_MONITOR


class EpochLogger(keras.callbacks.Callback):
    # Custom callback to log epoch progress in a readable format.
    
    def on_epoch_end(self, epoch, logs=None):
        logs = logs or {}
        loss = logs.get('loss', 0)
        val_loss = logs.get('val_loss', 0)
        mae = logs.get('mae', 0)
        val_mae = logs.get('val_mae', 0)
        
        # Improvement indicator
        improvement = " ✓ improving" if epoch > 0 and val_loss < self.prev_val_loss else ""
        self.prev_val_loss = val_loss
        
        print(f"\n  Epoch {epoch + 1} Summary:")
        print(f"    Train Loss: {loss:.4f} | Train MAE: {mae:.4f}°C")
        print(f"    Val Loss: {val_loss:.4f} | Val MAE: {val_mae:.4f}°C {improvement}")
    
    def on_train_begin(self, logs=None):
        self.prev_val_loss = float('inf')


def train_model(model, X_train, y_train, X_val, y_val):
    # Train the model with early stopping callback to prevent overfitting.
    # Monitors validation loss and stops if no improvement for EARLY_STOPPING_PATIENCE epochs.
    
    early_stop = keras.callbacks.EarlyStopping(
        monitor=EARLY_STOPPING_MONITOR,
        patience=EARLY_STOPPING_PATIENCE,
        restore_best_weights=True,
        verbose=1
    )
    
    epoch_logger = EpochLogger()
    
    history = model.fit(
        X_train, y_train,
        epochs=EPOCHS,
        batch_size=BATCH_SIZE,
        validation_data=(X_val, y_val),
        callbacks=[early_stop, epoch_logger],
        verbose=1
    )
    
    return history

