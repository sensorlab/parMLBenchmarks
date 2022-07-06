#!/usr/bin/env python
# coding: utf-8

# In[1]:


import time
import pandas as pd


# In[2]:


file_name = "C:/Users/zevni/Desktop/JSI/dejtafrejmi/emnist-digits-25000.csv"


# In[ ]:


for i in range(5):
    print("----------")
    start_time = time.time()
    data_1 = pd.read_csv(file_name)
    data_2 = pd.read_csv(file_name)
    print("--- %s seconds ---" % (time.time() - start_time))
    
    start_time = time.time()
    data = pd.concat([data_1, data_2], ignore_index=True)
    print("--- %s seconds ---" % (time.time() - start_time))
    print("----------")


# # SETOP TEST AND NOTIFY USER

# In[ ]:


def notification(frequency=200, length=1):
    global sound
    import IPython.display as ipd
    import numpy as np 
    beep = np.sin(np.pi*frequency*np.arange(10000*length)/10000)
    sound = ipd.Audio(beep, rate=10000, autoplay=True)
    return sound

notification()

