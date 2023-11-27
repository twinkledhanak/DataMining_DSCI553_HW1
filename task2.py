
# SparkContext is entrypoint to Spark, to connect to Spark clusters and create Spark RDD
from pyspark import SparkContext 
import json 

## For memory and time limits
import sys
from resource import *
import time
import psutil
import os 


os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3.6'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/local/bin/python3.6'

def process_memory():
    process = psutil.Process()
    memory_info = process.memory_info()
    memory_consumed = int(memory_info.rss/(1024*1024))
    return memory_consumed


start_time = time.time()

# SparkContext(Master with number of partitions ,AppName)
sc = SparkContext('local[*]','HW1_Task2') 

# Change logger level to print only ERROR logs
sc.setLogLevel("ERROR")

# Create a RDD from given data and transform each row to a json
args=sys.argv
#print("Arguments: ",args[0])

input_file_path = args[1] 
output_fle_path = args[2]
input_no_of_partitions = int(args[3])

def count_of_partition(iterator):
  yield sum(1 for x in iterator)

dataRDD = sc.textFile(str(input_file_path))
inputRDD = dataRDD.map(lambda row: json.loads(json.dumps(row)))


### DEFAULT no of partitions
start_time = time.time()
# F. The top 10 businesses that had the largest numbers of reviews and the number of reviews they had
dataForAllBusiness = inputRDD.map(lambda row: json.loads(row).get("business_id")).map(lambda business: (business,1)).collect()
parallelBData1 = sc.parallelize(dataForAllBusiness)
top_businessesRDD = parallelBData1.reduceByKey(lambda a,b: a+b)
top_businesses = top_businessesRDD.sortBy(lambda x: x[1],False).collect()
top10_business_sorted = []
for k,v in sorted(top_businesses,key=lambda x:(-x[1],x[0])):
    top10_business_sorted.append([k,v])
top10_business = top10_business_sorted[:10]    

parallelBData2 = sc.parallelize(parallelBData1.groupByKey().keys().collect())
finalBusiRDD = parallelBData2.map(lambda business: (1,1)).reduceByKey(lambda a,b: a+b).values().collect()
end_time = time.time()


### CUSTOMIZED no of partitions
start_time = time.time()
# F. The top 10 businesses that had the largest numbers of reviews and the number of reviews they had
dataForAllBusiness = inputRDD.map(lambda row: json.loads(row).get("business_id")).map(lambda business: (business,1)).collect()
parallelBData1 = sc.parallelize(dataForAllBusiness).partitionBy(input_no_of_partitions).persist()
top_businessesRDD = parallelBData1.reduceByKey(lambda a,b: a+b)
top_businesses = top_businessesRDD.sortBy(lambda row: row[1],False).collect()
top10_business_sorted = []
for k,v in sorted(top_businesses,key=lambda row:(-row[1],row[0])):
    top10_business_sorted.append([k,v])
top10_business = top10_business_sorted[:10]    

parallelBData2 = sc.parallelize(parallelBData1.groupByKey().keys().collect())
finalBusiRDD = parallelBData2.map(lambda business: (1,1)).reduceByKey(lambda a,b: a+b).values().collect()
end_time = time.time()


default = {}
default['n_partition'] = top_businessesRDD.getNumPartitions()
default['n_items'] = top_businessesRDD.mapPartitions(count_of_partition).collect()
default['exe_time'] = end_time - start_time


customized = {}
customized['n_partition'] = top_businessesRDD.getNumPartitions()
customized['n_items'] = top_businessesRDD.mapPartitions(count_of_partition).collect()
customized['exe_time'] = end_time - start_time


output_values = {}
output_values['default'] = default
output_values['customized'] = customized
with open(str(output_fle_path), "w") as outfile:
    json.dump(output_values, outfile)
#print("Output values : ",output_values)
#print("Total memory consumption in MBs: ",process_memory())