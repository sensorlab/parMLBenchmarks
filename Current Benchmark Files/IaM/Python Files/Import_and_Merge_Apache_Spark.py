#!/usr/bin/env python
# coding: utf-8

# # SETUP SPARK

# In[1]:


import time

import findspark
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import ShortType
from pyspark.sql.functions import col


# In[2]:


data_amount = 25000 #<-- this is half of the data set
file_name = "C:/Users/zevni/Desktop/JSI/dejtafrejmi/emnist-digits-" + str(data_amount) + ".csv"


# In[3]:


findspark.init("C:\spark")
spark = SparkSession.builder.master("spark://10.0.2.15:7077").config("spark.executor.memory", "8g").config("spark.driver.memory", "2g").config("spark.driver.memory", "2g").config("spark.sql.execution.arrow.pyspark.enabled","true").getOrCreate()


# # START TEST

# In[ ]:


for i in range(5):
    print("----------")
    start_time = time.time()
    data_a = spark.read.format('csv').load(file_name)
    data_b = spark.read.format('csv').load(file_name)
    print("--- %s seconds ---" % (time.time() - start_time))
    

    start_time = time.time()
    data = data_a.union(data_b)
    print("--- %s seconds ---" % (time.time() - start_time))

    start_time = time.time()
    data = data.toPandas()
    print("--- %s seconds ---" % (time.time() - start_time))
    print("----------")


# # SETOP TEST AND NOTIFY USER

# In[ ]:


spark.stop()
def notification(frequency=200, length=1):
    global sound
    import IPython.display as ipd
    import numpy as np 
    beep = np.sin(np.pi*frequency*np.arange(10000*length)/10000)
    sound = ipd.Audio(beep, rate=10000, autoplay=True)
    return sound

notification()


# In[5]:


spark.stop()

