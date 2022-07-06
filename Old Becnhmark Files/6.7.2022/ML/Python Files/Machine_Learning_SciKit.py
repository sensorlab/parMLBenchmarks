#!/usr/bin/env python
# coding: utf-8

# In[1]:


import time
from sklearn.tree import DecisionTreeClassifier
import numpy as np
import pandas as pd
from sklearn.metrics import classification_report


# # SETUP DATA

# In[2]:


data_amount = 250000
train_amount = int(data_amount*0.8)
test_amount = int(data_amount*0.2)


# In[ ]:


data = pd.read_csv("C:/Users/Filip/Desktop/zasluzbo/EMNIST/emnist-digits-train.csv")


# In[ ]:


arr_train = data.to_numpy()
arr_train  = np.concatenate((arr_train,arr_train),axis=0)

arr_test_label = arr_train[0:train_amount, 0]
arr_test_value = arr_train[0:train_amount, 1:]

arr_train_label = arr_train[train_amount:train_amount+test_amount, 0]
arr_train_value = arr_train[train_amount:train_amount+test_amount, 1:]


# # START TEST

# In[ ]:


for i in range(5):
    start_time = time.time()
    clf = DecisionTreeClassifier()
    clf.fit(arr_train_value, arr_train_label)
    predictions = clf.predict(arr_test_value)
    print(classification_report(arr_test_label, predictions))
    print("--- %s seconds ---" % (time.time() - start_time))


# # STOP TEST

# In[ ]:


def notification(frequency=200, length=5):
    global sound
    import IPython.display as ipd
    import numpy as np 
    beep = np.sin(np.pi*frequency*np.arange(10000*length)/10000)
    sound = ipd.Audio(beep, rate=10000, autoplay=True)
    return sound

notification()

