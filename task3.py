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


# SparkContext(Master with number of partitions ,AppName)
sc = SparkContext('local[*]','HW1_Task3') 

# Change logger level to print only ERROR logs
sc.setLogLevel("ERROR")

# Create a RDD from given data and transform each row to a json
args=sys.argv
review_input_file = str(args[1])
business_input_file = str(args[2])
output_file_1 = str(args[3])
output_file_2 = str(args[4])


# Function to write output for file
def generate_output1_file(avgValues):
    with open(output_file_1, "w") as outfile:
        outfile.write("city,stars")
        for record in avgValues:
            city,stars = record
            outfile.write('\n')
            outfile.write(str(city)+","+str(stars))  

# Function for Python sorting
def sort_using_python(elem):
  return elem[1]

# Reading test_review.json
input1RDD = sc.textFile(str(review_input_file))
reviewRDD = input1RDD.map(lambda row: json.loads(json.dumps(row))).map(lambda row:(json.loads(row).get("business_id"),
json.loads(row).get("stars")))


# Reading business.json
input2RDD = sc.textFile(str(business_input_file))
businessRDD = input2RDD.map(lambda row: json.loads(json.dumps(row))).map(lambda row:(json.loads(row).get("business_id"),
json.loads(row).get("city")))

# Merging both RDDs to aggregate and then compute average value of stars for each star
jointRDD = reviewRDD.join(businessRDD)
tempMap = jointRDD.map(lambda row:(row[1][1],int(row[1][0]))).groupByKey()
avgValues = tempMap.mapValues(lambda row: sum(row)/len(row))
python_values = avgValues.collect()

# First method m1 using python sort
start_time = time.time()
python_values.sort(reverse = True, key = sort_using_python)
python_values[:10]
end_time = time.time()
m1_duration = end_time-start_time

# Second method m2 using RDD
start_time = time.time()
avgValues = avgValues.sortBy(lambda row: (-row[1],row[0]))
temp = avgValues.top(10)
end_time = time.time()
m2_duration = end_time-start_time
avgValues=avgValues.collect()

#@TODO - CHange the reason here
generate_output1_file(avgValues)
output_values = {}
output_values['m1'] = m1_duration
output_values['m2'] = m2_duration
output_values['reason'] = "From our observations for time taken by both methods, we see that sorting using RDD is more efficient as compared to sorting via python. RDD works in-parallel on sorting small chunks of a larger dataset which makes it faster. For small size of datasets, both python and RDD sort would give similar performance however in case of larger datasets, using RDD sort is much more efficient. "
with open(output_file_2,"w") as outfile:
    json.dump(output_values,outfile)


#print("Total memory consumption in MBs: ",process_memory())