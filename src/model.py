from tensorflow import keras
from keras.models import Sequential
from keras import layers
import numpy


def build_model(x, y):
    """
    Build a model predicting y values (labels) from a set of x (features)
    x: NumpyArray
    y: NumpyArray
    """
    model = Sequential()

    # the input is multiple nodes, with each node contain 90 days' worth of data (hence the shape)
    model.add(layers.LSTM(100, return_sequences=True, input_shape=(x_train.shape[1],1)))
    
    # add another layer ?
    model.add(layers.LSTM(100, return_sequences=True, dropout=0.2))
    # add another LSTM layer
    model.add(layers.LSTM(100, return_sequences=False, dropout=0.2))
    
    # add densely connected layers
    model.add(layers.Dense(25))
    model.add(layers.Dense(1))
    
    # show summary of the architecture
    model.summary()
    
    # batch size is the number of samples used to train before the model is updated
    # epochs is when an entire dataset is passed forward and backward through the network once
    model.compile(optimizer='adam', loss='mean_squared_error', metrics=['accuracy'])
    model.fit(x, y, batch_size=10, epochs=5)
    return model