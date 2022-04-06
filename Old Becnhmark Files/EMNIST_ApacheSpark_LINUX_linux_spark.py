#!/usr/bin/env python
# coding: utf-8

# # SETUP THE DATA

import time

import findspark
import pyspark
import numpy as np
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import Row
from pyspark.sql import functions as f
from pyspark.sql.functions import rand
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator


data_amount = 250000
train_amount = data_amount * 0.8
test_amount = data_amount * 0.2


findspark.init("/opt/spark-3.2.0-bin-hadoop3.2")
spark = SparkSession.builder.master("spark://filip-VirtualBox:7077").getOrCreate()
#.master("spark://filip-VirtualBox:7077")


data_train = spark.read.csv("/media/sf_JSI/EMNIST/emnist-digits-train.csv", inferSchema=True).limit(train_amount)
data_test = spark.read.csv("/media/sf_JSI/EMNIST/emnist-mnist-train.csv", inferSchema=True).limit(test_amount)


print(data_train.count())
print(data_test.count())


feature_columns = data_train.columns[1:]
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
data_train_2 = assembler.transform(data_train)

feature_columns = data_test.columns[1:]
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
data_test_2 = assembler.transform(data_test)


#spark.stop()


# # START TRAINING

start_time = time.time()

algo = DecisionTreeClassifier(featuresCol="features", labelCol="_c0")
model = algo.fit(data_train_2)

predictions = model.transform(data_test_2)

evaluator = MulticlassClassificationEvaluator(predictionCol="prediction", labelCol="_c0")
accuracy = evaluator.evaluate(predictions)
print("This is F1: ")
print(accuracy)
print("--- %s seconds ---" % (time.time() - start_time))

#-----------------------------------------

start_time = time.time()

algo = DecisionTreeClassifier(featuresCol="features", labelCol="_c0")
model = algo.fit(data_train_2)

predictions = model.transform(data_test_2)

evaluator = MulticlassClassificationEvaluator(predictionCol="prediction", labelCol="_c0")
accuracy = evaluator.evaluate(predictions)
print("This is F1: ")
print(accuracy)
print("--- %s seconds ---" % (time.time() - start_time))

#-----------------------------------------

start_time = time.time()

algo = DecisionTreeClassifier(featuresCol="features", labelCol="_c0")
model = algo.fit(data_train_2)

predictions = model.transform(data_test_2)

evaluator = MulticlassClassificationEvaluator(predictionCol="prediction", labelCol="_c0")
accuracy = evaluator.evaluate(predictions)
print("This is F1: ")
print(accuracy)
print("--- %s seconds ---" % (time.time() - start_time))

#-----------------------------------------

start_time = time.time()

algo = DecisionTreeClassifier(featuresCol="features", labelCol="_c0")
model = algo.fit(data_train_2)

predictions = model.transform(data_test_2)

evaluator = MulticlassClassificationEvaluator(predictionCol="prediction", labelCol="_c0")
accuracy = evaluator.evaluate(predictions)
print("This is F1: ")
print(accuracy)
print("--- %s seconds ---" % (time.time() - start_time))

#-----------------------------------------

start_time = time.time()

algo = DecisionTreeClassifier(featuresCol="features", labelCol="_c0")
model = algo.fit(data_train_2)

predictions = model.transform(data_test_2)

evaluator = MulticlassClassificationEvaluator(predictionCol="prediction", labelCol="_c0")
accuracy = evaluator.evaluate(predictions)
print("This is F1: ")
print(accuracy)
print("--- %s seconds ---" % (time.time() - start_time))

#-----------------------------------------

# # STOP SPARK AND NOTIFY USER

spark.stop()
def notification(frequency=200, length=3):
    global sound
    import IPython.display as ipd
    import numpy as np 
    beep = np.sin(np.pi*frequency*np.arange(10000*length)/10000)
    sound = ipd.Audio(beep, rate=10000, autoplay=True)
    return sound

notification()


# In[ ]:




