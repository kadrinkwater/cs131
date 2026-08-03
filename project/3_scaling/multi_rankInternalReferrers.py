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

#All months for which we have data.
#I tried several different ways to check for file existence
#or catch nonexistent-file exceptions within gcloud/pyspark, and I give up
#for now. Because of the partial years on each end of the list, it's clunky
#to generate this with a for-loop.
alldates = ["2017-11","2017-12","2018-01","2018-02","2018-03","2018-04","2018-05","2018-06","2018-07","2018-08","2018-09","2018-10","2018-11","2018-12","2019-01",
"2019-02","2019-03","2019-04","2019-05","2019-06","2019-07","2019-08","2019-09","2019-10","2019-11","2019-12","2020-01","2020-02","2020-03","2020-04","2020-05",
"2020-06","2020-07","2020-08","2020-09","2020-10","2020-11","2020-12","2021-01","2021-02","2021-03","2021-04","2021-05","2021-06","2021-07","2021-08","2021-09",
"2021-10","2021-11","2021-12","2022-01","2022-02","2022-03","2022-04","2022-05","2022-06","2022-07","2022-08","2022-09","2022-10","2022-11","2022-12","2023-01",
"2023-02","2023-03","2023-04","2023-05","2023-06","2023-07","2023-08","2023-09","2023-10","2023-11","2023-12","2024-01","2024-02","2024-03","2024-04","2024-05",
"2024-06","2024-07","2024-08","2024-09","2024-10","2024-11","2024-12","2025-01","2025-02","2025-03","2025-04","2025-05","2025-06","2025-07","2025-08","2025-09",
"2025-10","2025-11","2025-12","2026-01","2026-02","2026-03","2026-04","2026-05","2026-06"]

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


#Start timer
startTime = time.time()


#Carry out analysis. I kinda want to see COVID emerge in this data so I'm
#slicing out the year-ish surrounding Jan 2020.
for yearmonth in alldates[20:33]:
	#Set up paths.
	#filenames are like clickstream-enwiki-2017-11.tsv
	inPath = f'gs://phonic-envoy-499516-i9-cs131/decompressed/clickstream-enwiki-{yearmonth}.tsv'
	outPath=f'gs://phonic-envoy-499516-i9-cs131/output/rank{yearmonth}'
	print(f"==== {yearmonth} ====")

	#Load data into DataFrame
	df = (
		spark_sesh.read.schema(schema).
		options(header=False, delimiter="\t")
		.csv(inPath, inferSchema=False)
	)

	#Check loaded correctly
	print(f"Total rows: {df.count():,}")

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