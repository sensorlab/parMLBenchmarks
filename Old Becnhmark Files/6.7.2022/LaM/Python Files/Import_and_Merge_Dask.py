#!/usr/bin/env python
# coding: utf-8

# In[1]:


import time

from dask.distributed import Client, progress
import dask.dataframe as dd
import pandas as pd


# In[2]:


file_name = "C:/Users/Filip\Desktop/zasluzbo/DASK/dejtafrejmi/emnist-digits-125000.csv"
threads = 8
workers = 1
mem_limit = '10G'


# In[ ]:


client = Client(threads_per_worker=threads,
                n_workers=workers, memory_limit=mem_limit)
client


# In[ ]:


for i in range(5):
    print("----------")
    start_time = time.time()
    data1 = dd.read_csv(file_name)
    data2 = dd.read_csv(file_name)
    print("--- %s seconds ---" % (time.time() - start_time))

    start_time = time.time()
    data1 = data1.astype("ushort")
    data2 = data2.astype("ushort")
    print("--- %s seconds ---" % (time.time() - start_time))
    
    start_time = time.time()
    #tle sm zamenu .append() z .concat()
    #in dodal ignore_index = True
    result = dd.concat([data1, data2], ignore_index=True)
    print("--- %s seconds ---" % (time.time() - start_time))

    start_time = time.time()
    result = result.compute()
    print("--- %s seconds ---" % (time.time() - start_time))
    print("----------")


# # SETOP TEST AND NOTIFY USER

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

