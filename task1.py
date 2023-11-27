
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
sc = SparkContext('local[*]','HW1_Task1') 

# Change logger level to print only ERROR logs
sc.setLogLevel("ERROR")

# Create a RDD from given data and transform each row to a json
args=sys.argv
#print("Arguments: ",args[0])

input_file_path = args[1]
output_fle_path = args[2]


dataRDD = sc.textFile(str(input_file_path))
inputRDD = dataRDD.map(lambda row: json.loads(json.dumps(row)))

# A. The total number of reviews 
n_review = inputRDD.count()

# B. The number of reviews in 2018
n_review_2018 = inputRDD.filter(lambda row: json.loads(row).get("date").split('-')[0] == "2018").count()

#n_user = inputRDD.map(lambda row: json.loads(row).get("user_id")).distinct().collect() # -- This works, but will lead to performance 
# issues with huge dataset. To avoid, we have to use two-step logic from class notes

# C. The number of distinct users who wrote reviews  
dataForAllUsers = inputRDD.map(lambda row: json.loads(row).get("user_id")).map(lambda user: (user, 1)).collect()
parallelData1 = sc.parallelize(dataForAllUsers) # This creates a new parallel data


# D. The top 10 users who wrote the largest numbers of reviews and the number of reviews they wrote
top_users = parallelData1.reduceByKey(lambda a,b: a+b).sortBy(lambda x: x[1],False).collect()
top10_user_sorted = []
for k,v in sorted(top_users,key=lambda x:(-x[1],x[0])):
    top10_user_sorted.append([k,v])
top10_user = top10_user_sorted[:10]    

# We again create a parallelized RDD and apply map-reduce for phase2
parallelData2 = sc.parallelize(parallelData1.groupByKey().keys().collect())
finalUserRDD = parallelData2.map(lambda user: (1,1)).reduceByKey(lambda a,b: a+b).values().collect() # map(1, no of users)


#E. The number of distinct businesses that have been reviewed
dataForAllBusiness = inputRDD.map(lambda row: json.loads(row).get("business_id")).map(lambda business: (business,1)).collect()
parallelBData1 = sc.parallelize(dataForAllBusiness)

# F. The top 10 businesses that had the largest numbers of reviews and the number of reviews they had
top_businesses = parallelBData1.reduceByKey(lambda a,b: a+b).sortBy(lambda x: x[1],False).collect()
top10_business_sorted = []
for k,v in sorted(top_businesses,key=lambda x:(-x[1],x[0])):
    top10_business_sorted.append([k,v])
top10_business = top10_business_sorted[:10]    

parallelBData2 = sc.parallelize(parallelBData1.groupByKey().keys().collect())
finalBusiRDD = parallelBData2.map(lambda business: (1,1)).reduceByKey(lambda a,b: a+b).values().collect()


output_values = {
    "n_review" : int(n_review), # done
    "n_review_2018" : int(n_review_2018), # done
    "n_user" : int(finalUserRDD[0]), # done
    "top10_user" : top10_user,
    "n_business" : int(finalBusiRDD[0]),
    "top10_business" : top10_business
}
with open(str(output_fle_path), "w") as outfile:
    json.dump(output_values, outfile)

end_time = time.time()
millis = (end_time - start_time)*1000
minutes=(millis/(1000*60))%60
#print("Total time taken in Minutes: ",minutes)
#print("Total memory consumption in MBs: ",process_memory())