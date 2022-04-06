#!/usr/bin/env python
# coding: utf-8

import time

from dask.distributed import Client, progress
from sklearn.tree import DecisionTreeClassifier
from sklearn.model_selection import GridSearchCV
import dask.dataframe as dd

import numpy as np
import pandas as pd

threads = 1
workers = 10
mem_limit = '5GB'

data_amount = 250000
train_amount = data_amount * 0.8
test_amount = data_amount * 0.2



client = Client(threads_per_worker=threads,
                n_workers=workers, memory_limit=mem_limit)
client


#client.close()


# # SETUP DATA

data = dd.read_csv("C:/Users/zevni/Desktop/JSI/EMNIST/emnist-digits-train.csv")


data_arr = data.to_dask_array(lengths=True)
data_arr = np.concatenate((data_arr, data_arr),axis=0)


xtrain = data_arr[0:train_amount, 1:]
train_label = data_arr[0:train_amount, 0]

xtest = data_arr[train_amount:train_amount + test_amount, 1:]
test_label = data_arr[train_amount:train_amount + test_amount, 0] 


# # START TEST

start_time = time.time()
paramaters = {}
clf =  DecisionTreeClassifier()
grid_search = GridSearchCV(clf, paramaters)
from sklearn.utils import parallel_backend

with parallel_backend('dask'):
    grid_search.fit(xtrain, train_label)
    predictions = grid_search.predict(xtest)
    
from sklearn.metrics import classification_report
print(classification_report(test_label, predictions))

print("--- %s seconds ---" % (time.time() - start_time))

client.close()

#-----------------------------------------

client = Client(threads_per_worker=threads,
                n_workers=workers, memory_limit=mem_limit)

start_time = time.time()
paramaters = {}
clf =  DecisionTreeClassifier()
grid_search = GridSearchCV(clf, paramaters)
from sklearn.utils import parallel_backend

with parallel_backend('dask'):
    grid_search.fit(xtrain, train_label)
    predictions = grid_search.predict(xtest)
    
from sklearn.metrics import classification_report
print(classification_report(test_label, predictions))

print("--- %s seconds ---" % (time.time() - start_time))

client.close()

#-----------------------------------------

client = Client(threads_per_worker=threads,
                n_workers=workers, memory_limit=mem_limit)

start_time = time.time()
paramaters = {}
clf =  DecisionTreeClassifier()
grid_search = GridSearchCV(clf, paramaters)
from sklearn.utils import parallel_backend

with parallel_backend('dask'):
    grid_search.fit(xtrain, train_label)
    predictions = grid_search.predict(xtest)
    
from sklearn.metrics import classification_report
print(classification_report(test_label, predictions))

print("--- %s seconds ---" % (time.time() - start_time))

client.close()

#-----------------------------------------

client = Client(threads_per_worker=threads,
                n_workers=workers, memory_limit=mem_limit)

start_time = time.time()
paramaters = {}
clf =  DecisionTreeClassifier()
grid_search = GridSearchCV(clf, paramaters)
from sklearn.utils import parallel_backend

with parallel_backend('dask'):
    grid_search.fit(xtrain, train_label)
    predictions = grid_search.predict(xtest)
    
from sklearn.metrics import classification_report
print(classification_report(test_label, predictions))

print("--- %s seconds ---" % (time.time() - start_time))

client.close()

#-----------------------------------------

client = Client(threads_per_worker=threads,
                n_workers=workers, memory_limit=mem_limit)

start_time = time.time()
paramaters = {}
clf =  DecisionTreeClassifier()
grid_search = GridSearchCV(clf, paramaters)
from sklearn.utils import parallel_backend

with parallel_backend('dask'):
    grid_search.fit(xtrain, train_label)
    predictions = grid_search.predict(xtest)
    
from sklearn.metrics import classification_report
print(classification_report(test_label, predictions))

print("--- %s seconds ---" % (time.time() - start_time))

client.close()

#-----------------------------------------

# # END TEST

def notification(frequency=200, length=5):
    global sound
    import IPython.display as ipd
    import numpy as np 
    beep = np.sin(np.pi*frequency*np.arange(10000*length)/10000)
    sound = ipd.Audio(beep, rate=10000, autoplay=True)
    return sound

notification()

