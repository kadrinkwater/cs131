#Examine internal referrals, i.e. a Wiki user clicking a link
#from one wiki page to another.
#Exclude external referrals (new tab, search)
#Exclude "main" pages

import sys, time
from pyspark.sql import SparkSession
import pyspark.sql.functions as fns

#Define data schema. NOTE: renamed 'type' to 'ctype' because type is
#a keyword in python
schema = "prev STRING, curr STRING, ctype STRING, clicks LONG"

#Create SparkSession
spark_sesh = SparkSession.builder.appName("search_proportion").getOrCreate()

def rankInternalReferrers(df, n=100):
  """
  Returns a DataFrame of the top n internal referrers
  (other than Wiki[pedia] and Main_Page which are uninteresting)
  """
  exclude = ["Wiki", "Wikipedia", "Main_Page"]
  newdf = (
      df.filter(df.ctype == "link")   #Wiki-internal link clicks only
      .filter(~df.prev.isin(exclude)) #exclude from list above
      .groupBy(df.prev)               #Sum clicks for each referrer
      .agg({'clicks': 'sum'})
      .sort("sum(clicks)", ascending=False) #Sort desc & take top n
      .limit(n)
  )
  return newdf

inPath = sys.argv[1]
#Extract the year-month from the filename
#which are like clickstream-enwiki-2017-11.tsv
yearmonth = inPath[-11:-4]
outPath=f'gs://phonic-envoy-499516-i9-cs131/output/rank{yearmonth}'
print(f"==== {yearmonth} ====")

#Start timer
startTime = time.time()

#Load data into DataFrame
df = (
	spark_sesh.read.schema(schema).
	options(header=False, delimiter="\t")
	.csv(inPath, inferSchema=False)
)

#Check loaded correctly
#print(f"Total rows: {df.count():,}")

#Carry out ranking computation
result = rankInternalReferrers(df, 15)

#Write out result
result.write.mode("overwrite").csv(outPath, header=True, sep='\t')
print(f"Result written to {outPath}")
print("Top 15:")
result.show(truncate=40)

#Stop timer
computeTime = time.time() - startTime
#Get num executors
executors = spark_sesh.conf.get("spark.executor.instances", "default")

#Print output
print(f"{computeTime:.2f} sec on {executors} executors")
print("")

#Stop SparkSession
spark_sesh.stop()