# Analyze proportion of clicks coming from other-search
# for a given month of clickstream data.

import sys, time
from pyspark.sql import SparkSession
import pyspark.sql.functions as fns

# def countClicksFromPrev(frame, prev=""):
#   """
#   Takes a DataFrame and returns the sum of all clicks for
#   the specified prev. If prev is not specified, sum all clicks
#   for whole DataFrame.
#   """
#   if prev == "":
#     newframe = frame
#   else:
#     newframe = frame.filter(frame.prev == prev)
#   sumdf = newframe.select(fns.sum(newframe.clicks).alias('s'))
#   sumint = sumdf.collect()[0].s
#   return sumint


def sumClicks(frame):
  """
  Sum the 'clicks' column for an entire DataFrame, then
  extract and return the result as an int.
  """
  sumdf = frame.select(fns.sum(frame.clicks).alias('s'))
  sumint = sumdf.collect()[0].s
  return sumint




#Create SparkSession
spark_sesh = SparkSession.builder.appName("search_proportion").getOrCreate()

#Path to data file will be given as command line argument.
#Also extract the year-month from the filename
#which are like clickstream-enwiki-2017-11.tsv
path = sys.argv[1]
yearmonth = path[-11:-4]

#Define data schema. NOTE: renamed 'type' to 'ctype' because type is
#a keyword in python
schema = "prev STRING, curr STRING, ctype STRING, clicks LONG"


#Start timer
startTime = time.time()


#Load data into DataFrame
df = spark_sesh.read.schema(schema).options(header=False, delimiter="\t").csv(path, inferSchema=False)

#Check loaded correctly
df.printSchema()
print("Total rows: ", df.count())


#Calculate number of clicks in total / from search
totalClicks = sumClicks(df)
searchClicks = sumClicks(df.filter(df.prev == "other-search"))
proportion = (searchClicks/totalClicks)*100

#Stop timer
computeTime = time.time() - startTime
#Get num executors
executors = spark_sesh.conf.get("spark.executor.instances", "default")

#Print output
# output = f"Total clicks: {tot:,} / Clicks from search: {srch:,} ({frac:.1f}%)"
# print(output)
print(f"==== {yearmonth} ====")
print(f"Total clicks: {totalClicks:,}")
print(f"Search clicks: {searchClicks:,}")
print(f"{proportion:.2f} from search")
print(f"{computeTime:.2f} sec on {executors} executors")
print("")


#Stop SparkSession
spark_sesh.stop()