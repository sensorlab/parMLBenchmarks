#!/usr/bin/env python
# coding: utf-8

# # SETUP SPARK

import time

import findspark
import pyspark
import numpy as np
from pyspark.sql import SparkSession


file_name = "C:/Users/Filip/Desktop/zasluzbo/EMNIST/emnist-digits-train.csv"
data_amount = 50000 #<-- actualy sample size devided by 2


findspark.init("C:\spark")
spark = SparkSession.builder.master("spark://10.0.2.15:7077").getOrCreate()
#.master("spark://10.0.2.15:7077")


# # START TEST
#-----------------------------------------

start_time = time.time()
data_a = spark.read.csv(file_name, inferSchema=True).limit(data_amount)
data_b = spark.read.csv(file_name, inferSchema=True).limit(data_amount)
print("--- %s seconds ---" % (time.time() - start_time))

start_time = time.time()
data = data_a.unionAll(data_b)
print("--- %s seconds ---" % (time.time() - start_time))

start_time = time.time()
data.toPandas()
print("--- %s seconds ---" % (time.time() - start_time))

#-----------------------------------------

start_time = time.time()
data_a = spark.read.csv(file_name, inferSchema=True).limit(data_amount)
data_b = spark.read.csv(file_name, inferSchema=True).limit(data_amount)
print("--- %s seconds ---" % (time.time() - start_time))

start_time = time.time()
data = data_a.unionAll(data_b)
print("--- %s seconds ---" % (time.time() - start_time))

start_time = time.time()
data.toPandas()
print("--- %s seconds ---" % (time.time() - start_time))

#-----------------------------------------

start_time = time.time()
data_a = spark.read.csv(file_name, inferSchema=True).limit(data_amount)
data_b = spark.read.csv(file_name, inferSchema=True).limit(data_amount)
print("--- %s seconds ---" % (time.time() - start_time))

start_time = time.time()
data = data_a.unionAll(data_b)
print("--- %s seconds ---" % (time.time() - start_time))

start_time = time.time()
data.toPandas()
print("--- %s seconds ---" % (time.time() - start_time))

#-----------------------------------------

start_time = time.time()
data_a = spark.read.csv(file_name, inferSchema=True).limit(data_amount)
data_b = spark.read.csv(file_name, inferSchema=True).limit(data_amount)
print("--- %s seconds ---" % (time.time() - start_time))

start_time = time.time()
data = data_a.unionAll(data_b)
print("--- %s seconds ---" % (time.time() - start_time))

start_time = time.time()
data.toPandas()
print("--- %s seconds ---" % (time.time() - start_time))

#-----------------------------------------

start_time = time.time()
data_a = spark.read.csv(file_name, inferSchema=True).limit(data_amount)
data_b = spark.read.csv(file_name, inferSchema=True).limit(data_amount)
print("--- %s seconds ---" % (time.time() - start_time))

start_time = time.time()
data = data_a.unionAll(data_b)
print("--- %s seconds ---" % (time.time() - start_time))

start_time = time.time()
data.toPandas()
print("--- %s seconds ---" % (time.time() - start_time))

#-----------------------------------------
# # SETOP TEST AND NOTIFY USER


spark.stop()
def notification(frequency=200, length=5):
    global sound
    import IPython.display as ipd
    import numpy as np 
    beep = np.sin(np.pi*frequency*np.arange(10000*length)/10000)
    sound = ipd.Audio(beep, rate=10000, autoplay=True)
    return sound

notification()

