#!/usr/bin/env python
# coding: utf-8

import time

from dask.distributed import Client, progress
import dask.dataframe as dd


file_name = "C:/Users/Filip/Desktop/zasluzbo/DASK/dejtafrejmi/emnist-digits-125000.csv"
threads = 1
workers = 10
mem_limit = '1.2GB'

#-----------------------------------------

client = Client(threads_per_worker=threads,
                n_workers=workers, memory_limit=mem_limit)
client


start_time = time.time()
data1 = dd.read_csv(file_name)
data2 = dd.read_csv(file_name)
print("--- %s seconds ---" % (time.time() - start_time))

start_time = time.time()
result = data1.append(data2)
print("--- %s seconds ---" % (time.time() - start_time))

start_time = time.time()
result = result.compute()
print("--- %s seconds ---" % (time.time() - start_time))
client.close()

#-----------------------------------------

client = Client(threads_per_worker=threads,
                n_workers=workers, memory_limit=mem_limit)
start_time = time.time()
data1 = dd.read_csv(file_name)
data2 = dd.read_csv(file_name)
print("--- %s seconds ---" % (time.time() - start_time))

start_time = time.time()
result = data1.append(data2)
print("--- %s seconds ---" % (time.time() - start_time))

start_time = time.time()
result = result.compute()
print("--- %s seconds ---" % (time.time() - start_time))
client.close()

#-----------------------------------------

client = Client(threads_per_worker=threads,
                n_workers=workers, memory_limit=mem_limit)
start_time = time.time()
data1 = dd.read_csv(file_name)
data2 = dd.read_csv(file_name)
print("--- %s seconds ---" % (time.time() - start_time))

start_time = time.time()
result = data1.append(data2)
print("--- %s seconds ---" % (time.time() - start_time))

start_time = time.time()
result = result.compute()
print("--- %s seconds ---" % (time.time() - start_time))
client.close()

#-----------------------------------------

client = Client(threads_per_worker=threads,
                n_workers=workers, memory_limit=mem_limit)
start_time = time.time()
data1 = dd.read_csv(file_name)
data2 = dd.read_csv(file_name)
print("--- %s seconds ---" % (time.time() - start_time))

start_time = time.time()
result = data1.append(data2)
print("--- %s seconds ---" % (time.time() - start_time))

start_time = time.time()
result = result.compute()
print("--- %s seconds ---" % (time.time() - start_time))
client.close()

#-----------------------------------------

client = Client(threads_per_worker=threads,
                n_workers=workers, memory_limit=mem_limit)
start_time = time.time()
data1 = dd.read_csv(file_name)
data2 = dd.read_csv(file_name)
print("--- %s seconds ---" % (time.time() - start_time))

start_time = time.time()
result = data1.append(data2)
print("--- %s seconds ---" % (time.time() - start_time))

start_time = time.time()
result = result.compute()
print("--- %s seconds ---" % (time.time() - start_time))
client.close()

