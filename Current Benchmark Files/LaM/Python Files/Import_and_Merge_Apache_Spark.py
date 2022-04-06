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


file_name = "C:/Users/Filip\Desktop/zasluzbo/EMNIST/emnist-digits-train.csv"
data_amount = 25000 #<-- tle izberes polovico seta


# In[3]:


findspark.init("C:\spark")
spark = SparkSession.builder.master("spark://10.0.2.15:7077").config("spark.executor.memory", "8g").config("spark.driver.memory", "2g").getOrCreate()


# # START TEST

# In[ ]:


for i in range(5):
    print("----------")
    start_time = time.time()
    data_a = spark.read.csv(file_name, inferSchema=True).limit(data_amount)
    data_b = spark.read.csv(file_name, inferSchema=True).limit(data_amount)
    print("--- %s seconds ---" % (time.time() - start_time))
    
    start_time = time.time()
    data_a = data_a.select([col(c).cast(ShortType()) for c in data_a.columns])
    data_b = data_b.select([col(c).cast(ShortType()) for c in data_b.columns])
    print("--- %s seconds ---" % (time.time() - start_time))
    

    start_time = time.time()
    data = data_a.union(data_b) #tle sm zamenu .unionAll() z .union()
    print("--- %s seconds ---" % (time.time() - start_time))

    start_time = time.time()
    data = data.toPandas()
    print("--- %s seconds ---" % (time.time() - start_time))
    print("----------")


# # SETOP TEST AND NOTIFY USER

# In[ ]:


spark.stop()
def notification(frequency=200, leng89th=5):
    global sound
    import IPython.display as ipd
    import numpy as np 
    beep = np.sin(np.pi*frequency*np.arange(10000*length)/10000)
    sound = ipd.Audio(beep, rate=10000, autoplay=True)
    return sound

notification()


# In[5]:


spark.stop()

