# Configuration and constants for the ML pipeline.

import os

# Reproducibility
SEED = 42

# Data paths
DATA_PATH = os.path.join(os.path.dirname(__file__), '../data/observations_06102.csv')
MODEL_PATH = os.path.join(os.path.dirname(__file__), '../models/model.keras')
METADATA_PATH = os.path.join(os.path.dirname(__file__), '../models/metadata.pkl')

# Feature and target configuration
TARGET_COLUMN = 'temp_dry'
HORIZONS = list(range(1, 73))  # every hour ahead from 1 to 72

# Train/validation/test split
TRAIN_RATIO = 0.70
VAL_RATIO = 0.15
TEST_RATIO = 0.15

# Model hyperparameters
BATCH_SIZE = 32
EPOCHS = 50  # Will be limited by early stopping
LEARNING_RATE = 0.001
DROPOUT_RATE = 0.15

# Training configuration
EARLY_STOPPING_PATIENCE = 5
EARLY_STOPPING_MONITOR = 'val_loss'
