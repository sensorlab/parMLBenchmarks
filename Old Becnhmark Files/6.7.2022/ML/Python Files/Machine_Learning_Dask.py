#!/usr/bin/env python
# coding: utf-8

# In[1]:


import time

from dask.distributed import Client, progress
from sklearn.tree import DecisionTreeClassifier
import dask.dataframe as dd
from sklearn.utils import parallel_backend
from sklearn.metrics import classification_report

import numpy as np
import pandas as pd


# In[ ]:


threads = 1
workers = 8
mem_limit = '1.25G'

data_amount = 250000
train_amount = int(data_amount * 0.8)
test_amount =int(data_amount * 0.2)

print(train_amount)
print(test_amount)


# In[ ]:


client = Client(threads_per_worker=threads,
                n_workers=workers, memory_limit=mem_limit)
client


# In[4]:


#client.close()


# # SETUP DATA

# In[7]:


data = dd.read_csv("C:/Users/zevni/Desktop/JSI/EMNIST/emnist-digits-train.csv")


# In[8]:


data_arr = data.to_dask_array(lengths=True)
data_arr = np.concatenate((data_arr, data_arr),axis=0)


# In[9]:


data_arr = data_arr.astype("ushort")


# In[10]:


xtrain = data_arr[0:train_amount, 1:]
train_label = data_arr[0:train_amount, 0]

xtest = data_arr[train_amount:train_amount + test_amount, 1:]
test_label = data_arr[train_amount:train_amount + test_amount, 0] 


# # START TEST

# In[ ]:


for i in range(5):
    start_time = time.time()
    
    clf =  DecisionTreeClassifier(max_depth=5)
    
    with parallel_backend('dask'):
        clf.fit(xtrain, train_label)
        predictions = clf.predict(xtest)
    
    print(classification_report(test_label, predictions))
    
    print("--- %s seconds ---" % (time.time() - start_time))


# # END TEST

# In[ ]:


client.close()
def notification(frequency=200, length=5):
    global sound
    import IPython.display as ipd
    import numpy as np 
    beep = np.sin(np.pi*frequency*np.arange(10000*length)/10000)
    sound = ipd.Audio(beep, rate=10000, autoplay=True)
    return sound

notification()


# In[ ]:


#client.close()

