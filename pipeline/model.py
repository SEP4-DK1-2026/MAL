# Step 4a: Model architecture definition.

import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers
from pipeline.config import DROPOUT_RATE, LEARNING_RATE


def build_model(input_dim, output_dim):
    # Build a regression model using Dense layers with ReLU activation and dropout.
    # Architecture: Input -> Dense(128, ReLU) -> Dropout -> Dense(64, ReLU) ->
    # Dense(32, ReLU) -> Dense(output_dim, linear).
    # Uses Adam optimizer with MSE loss (standard for regression).
    
    model = keras.Sequential([
        keras.Input(shape=(input_dim,)),
        layers.Dense(128, activation='relu'),
        layers.Dropout(DROPOUT_RATE),
        layers.Dense(64, activation='relu'),
        layers.Dense(32, activation='relu'),
        layers.Dense(output_dim, activation='linear')
    ])
    
    # Compile with Adam optimizer and MSE loss for regression
    model.compile(
        optimizer=keras.optimizers.Adam(learning_rate=LEARNING_RATE),
        loss='mse',
        metrics=['mae']
    )
    
    return model

