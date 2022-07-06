#!/usr/bin/env python
# coding: utf-8

# # SETUP THE DATA

# In[10]:


import time


import findspark
import pyspark
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import Row
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql.types import ShortType
from pyspark.sql.functions import col


# In[2]:


data_amount = 25000
train_amount = int(data_amount * 0.8)
test_amount = int(data_amount * 0.2)


# In[3]:


print(train_amount)
print(test_amount)


# In[4]:


findspark.init("C:\spark")
spark = SparkSession.builder.master("spark://10.0.2.15:7077").appName("Spark Windows 10").config("spark.executor.memory", "8g").config("spark.driver.memory", "2g").getOrCreate()


# In[5]:


data_train = spark.read.csv("C:/Users/Filip/Desktop/zasluzbo/EMNIST/emnist-digits-train.csv", inferSchema=True).limit(train_amount)
data_test = spark.read.csv("C:/Users/Filip/Desktop/zasluzbo/EMNIST/emnist-mnist-train.csv", inferSchema=True).limit(test_amount)


# In[11]:


data_train = data_train.select([col(c).cast(ShortType()) for c in data_train.columns])
data_test = data_test.select([col(c).cast(ShortType()) for c in data_test.columns])


# In[12]:


feature_columns = data_train.columns[1:]
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
data_train_2 = assembler.transform(data_train)

feature_columns = data_test.columns[1:]
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
data_test_2 = assembler.transform(data_test)


# In[13]:


#spark.stop()


# # START TRAINING

# In[ ]:


for i in range(5):
    start_time = time.time()

    algo = DecisionTreeClassifier(featuresCol="features", labelCol="_c0")
    model = algo.fit(data_train_2)

    predictions = model.transform(data_test_2)

    evaluator = MulticlassClassificationEvaluator(predictionCol="prediction", labelCol="_c0")
    accuracy = evaluator.evaluate(predictions)
    print("This is F1: ")
    print(accuracy)
    print("--- %s seconds ---" % (time.time() - start_time))


# # STOP SPARK AND NOTIFY USER

# In[ ]:


spark.stop()
def notification(frequency=200, length=5):
    global sound
    import IPython.display as ipd
    import numpy as np 
    beep = np.sin(np.pi*frequency*np.arange(10000*length)/10000)
    sound = ipd.Audio(beep, rate=10000, autoplay=True)
    return sound

notification()

