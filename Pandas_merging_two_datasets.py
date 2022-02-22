#!/usr/bin/env python
# coding: utf-8

import time
import pandas as pd


file_name = "C:/Users/Filip/Desktop/zasluzbo/DASK/dejtafrejmi/emnist-digits-125000.csv"

#-----------------------------------------

start_time = time.time()
data_1 = pd.read_csv(file_name)
data_2 = pd.read_csv(file_name)
print("--- %s seconds ---" % (time.time() - start_time))

start_time = time.time()
data = data_1.append(data_2)
print("--- %s seconds ---" % (time.time() - start_time))

#-----------------------------------------

start_time = time.time()
data_1 = pd.read_csv(file_name)
data_2 = pd.read_csv(file_name)
print("--- %s seconds ---" % (time.time() - start_time))

start_time = time.time()
data = data_1.append(data_2)
print("--- %s seconds ---" % (time.time() - start_time))

#-----------------------------------------

start_time = time.time()
data_1 = pd.read_csv(file_name)
data_2 = pd.read_csv(file_name)
print("--- %s seconds ---" % (time.time() - start_time))

start_time = time.time()
data = data_1.append(data_2)
print("--- %s seconds ---" % (time.time() - start_time))

#-----------------------------------------

start_time = time.time()
data_1 = pd.read_csv(file_name)
data_2 = pd.read_csv(file_name)
print("--- %s seconds ---" % (time.time() - start_time))

start_time = time.time()
data = data_1.append(data_2)
print("--- %s seconds ---" % (time.time() - start_time))

#-----------------------------------------

start_time = time.time()
data_1 = pd.read_csv(file_name)
data_2 = pd.read_csv(file_name)
print("--- %s seconds ---" % (time.time() - start_time))

start_time = time.time()
data = data_1.append(data_2)
print("--- %s seconds ---" % (time.time() - start_time))

