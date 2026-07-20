# Kell Drinkwater
# CS131 Su26
# ws5

#!pip install pyspark
#(because I tested this in a google colab first)


#A1. Create a SparkSession named ws5-regression.

import sys
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("ws5-regression").getOrCreate()



#A2. Read the dataset from your bucket into a DataFrame with the header row
#as column names and column types inferred, then .show() it.
#Don't hardcode the bucket — take the gs://.../tips.csv path as a command-line
#argument (sys.argv[1]).

path = sys.argv[1]
#path = "/content/tips.csv"
inputDF = spark.read.csv(path, header=True, inferSchema=True)

inputDF.printSchema()
print(f"Input Data Frame has {inputDF.count()} total records")
inputDF.show(5)



#A3. Combine the two predictor columns total_bill and size into a single
#vector column called features.

from pyspark.ml.feature import VectorAssembler
va = VectorAssembler(inputCols=["total_bill", "size"], outputCol="features")
vecDF = va.transform(inputDF)
print("Transformed data frame (total_bill + size vectorized):")
vecDF.select("total_bill", "size", "features", "tip").show(5)




#A4. Split the data into 80% train / 20% test. Pass a fixed seed so the
#split is reproducible. (Hint: .randomSplit().)

trainDF, testDF = inputDF.randomSplit([0.8, 0.2], seed=37)
print(f"Training set {trainDF.count()} rows, test set {testDF.count()} rows")



#A5. Define a LinearRegression with featuresCol="features" and labelCol="tip",
##and fit the model.
#Chain the assembler (A3) and the regressor into a Pipeline
#and call .fit() on the training set.

from pyspark.ml import Pipeline
from pyspark.ml.regression import LinearRegression
lr = LinearRegression(featuresCol="features", labelCol="tip")

pipeline = Pipeline(stages=[va, lr])
pipeModel = pipeline.fit(trainDF)



#A6. Apply the fitted pipeline to the test set to produce predictions.

predictDF = pipeModel.transform(testDF)

#(Check predictions)
print("Checking prediction DF: schema, predicted data")
predictDF.printSchema()
predictDF.select("total_bill", "size", "features", "tip", "prediction").show(5)




#A7. Evaluate the predictions on two metrics: RMSE and R². Use one evaluator
#with the label column tip, changing metricName.

from pyspark.ml.evaluation import RegressionEvaluator
regev = RegressionEvaluator(
    predictionCol="prediction",
    labelCol="tip",
    metricName="rmse"
)
rmseResult = regev.evaluate(predictDF)
regev.setMetricName("r2")
r2Result = regev.evaluate(predictDF)




#A8. Pull the fitted LinearRegression model out of the pipeline (pipelineModel.stages[-1])
#and print its coefficients and intercept, plus the RMSE and R² from A7.
#Use clear labels (e.g. print(f"RMSE: {rmse}")) so the numbers stand out in the job log.

fittedLR = pipeModel.stages[-1]
co1, co2 = fittedLR.coefficients
b = fittedLR.intercept

print(f"Model Coefficients: {co1:.2f}, {co2:.2f} / Intercept: {b:.2f}")
print(f"On test data: RMSE: {rmseResult:.2f} / R^2: {r2Result:.2f}")

summary = fittedLR.summary
print(f"On training data: RMSE: {summary.rootMeanSquaredError:.2f} / R^2: {summary.r2:.2f}")


