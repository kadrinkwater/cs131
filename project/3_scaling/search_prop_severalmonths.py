# Analyze proportion of clicks coming from other-search
# for several (hardcoded) months of clickstream data.

import sys, time, os
from pyspark.sql import SparkSession
import pyspark.sql.functions as fns


def sumClicks(frame):
  """
  Sum the 'clicks' column for an entire DataFrame, then
  extract and return the result as an int.
  """
  sumdf = frame.select(fns.sum(frame.clicks).alias('s'))
  sumint = sumdf.collect()[0].s
  return sumint


def analyzeMonth(path):
  #Check if path exists and is a file
  if not os.path.isfile(path):
    print(f"File does not exist (or is not a file): {path}")
    return


  #Extract the year-month from the filename
  #which are like clickstream-enwiki-2017-11.tsv
  #path = sys.argv[1]
  yearmonth = path[-11:-4]

  #Define data schema. NOTE: renamed 'type' to 'ctype' because type is
  #a keyword in python
  schema = "prev STRING, curr STRING, ctype STRING, clicks LONG"

  #Load data into DataFrame
  df = spark_sesh.read.schema(schema).options(header=False, delimiter="\t").csv(path, inferSchema=False)
  #Check loaded correctly
  print(f"==== {yearmonth} ====")
  print("Total rows: ", df.count())

  #Calculate number of clicks in total / from search
  totalClicks = sumClicks(df)
  searchClicks = sumClicks(df.filter(df.prev == "other-search"))
  proportion = (searchClicks/totalClicks)*100

  #Print output
  print(f"Total clicks: {totalClicks:,}")
  print(f"Search clicks: {searchClicks:,}")
  print(f"{proportion:.2f}% from search")



# Make list of file paths to analyze.
# For this run, we are doing all Octobers in the dataset.
# Paths are like [BUCKET]/decompressed/clickstream-enwiki-2017-11.tsv
years = [17, 18, 19, 20, 21, 22, 23, 24, 25, 26]
paths = [f'../decompressed/clickstream-enwiki-20{yy}-10.tsv' for yy in years]


#Create SparkSession
spark_sesh = SparkSession.builder.appName("search_proportion").getOrCreate()

#Start timer
startTime = time.time()

#Call analysis function
print("========= BEGIN ANALYSIS ==========")
for p in paths:
  analyzeMonth(p)

#Stop timer
computeTime = time.time() - startTime
#Get num executors
executors = spark_sesh.conf.get("spark.executor.instances", "default")

#Print output
print(f"{computeTime:.2f} sec on {executors} executors")
print("")


#Stop SparkSession
spark_sesh.stop()