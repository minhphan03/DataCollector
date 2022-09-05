import numpy as np
import pandas as pd
import math
from sklearn.preprocessing import MinMaxScaler
from model import build_model

pd.options.display.max_columns = 20
df = pd.read_csv("vni_data.csv", parse_dates=[0])

# reformat 
df['Price']=df['Price'].apply(lambda x: float(x.split()[0].replace(',', ''))).astype(float)
df['Open']=df['Open'].apply(lambda x: float(x.split()[0].replace(',', ''))).astype(float)
df['High']=df['High'].apply(lambda x: float(x.split()[0].replace(',', ''))).astype(float)
df['Low']=df['Low'].apply(lambda x: float(x.split()[0].replace(',', ''))).astype(float)
df['Vol.']=df['Vol.'].apply(lambda x: int(float(x[:-1])*1000)).astype(int)


# data preprocessing
cprice_values = df['Price'].values
training_data_len = math.ceil(len(cprice_values)*0.8)
scaler = MinMaxScaler(feature_range=(0,1))

# transform into 2D array (n rows, 1 column)
scaled_data = scaler.fit_transform(cprice_values.reshape(-1,1))
train_data = scaled_data[0: training_data_len, :]

x_train = []
y_train = []

# create windows for sequential training:
# first 60 days as feature (x)
# the 61st day as label (y)
for i in range(60, len(train_data)):
    x_train.append(train_data[i-60:i, 0])
    y_train.append(train_data[i, 0])
    
# convert to NumPy arrays
x_train, y_train = np.array(x_train), np.array(y_train)

# for a LSTM model, convert x_train into a 3D shape array
x_train = np.reshape(x_train, (x_train.shape[0], x_train.shape[1], 1))

# training data include the -60 days for the first few values in y test
test_data = scaled_data[training_data_len-60:, :]
x_test = []
y_test = np.array(scaled_data[training_data_len:])

for i in range(60, len(test_data)):
    # leave out the last date for y (since y > any x)
    x_test.append(test_data[i-60:i, 0])

x_test = np.array(x_test)
x_test = np.reshape(x_test, (x_test.shape[0], x_test.shape[1], 1))

# make sure that x and y has equal lengths
print(x_train.shape[0] == y_train.shape[0])
x_train.shape
print(x_test.shape[0] == y_test.shape[0])

# build model
model = build_model(x=x_train, y=y_train)
