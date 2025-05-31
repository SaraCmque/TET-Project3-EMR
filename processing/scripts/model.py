from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, round, current_timestamp
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor, GBTRegressor
from pyspark.ml import Pipeline
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import RegressionEvaluator

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

spark = SparkSession.builder.appName("RegressionModels").getOrCreate()

# Load data
df = spark.read.parquet("s3a://eafit-project-3-bucket/trusted/joined_data/*.parquet")
df = df.withColumnRenamed("reported_incidents", "label")

# Feature selection
assembler = VectorAssembler(inputCols=["max_temp", "precipitation", "congestion_level_num"],
                            outputCol="features")

# Regression models
rf = RandomForestRegressor(featuresCol="features", labelCol="label", seed=42)
gbt = GBTRegressor(featuresCol="features", labelCol="label", seed=42)

# Pipelines
pipeline_rf = Pipeline(stages=[assembler, rf])
pipeline_gbt = Pipeline(stages=[assembler, gbt])

# Hyperparameters
paramGrid_rf = (ParamGridBuilder()
                .addGrid(rf.numTrees, [50, 100, 150])
                .addGrid(rf.maxDepth, [5, 10, 15])
                .addGrid(rf.minInstancesPerNode, [1, 5, 10])
                .build())

paramGrid_gbt = (ParamGridBuilder()
                 .addGrid(gbt.maxIter, [50, 100])
                 .addGrid(gbt.maxDepth, [3, 5, 7])
                 .addGrid(gbt.stepSize, [0.01, 0.1])
                 .build())

# Evaluator with RMSE
evaluator = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse")

# CrossValidator configuration with K-Fold technique
cv_rf = CrossValidator(estimator=pipeline_rf,
                       estimatorParamMaps=paramGrid_rf,
                       evaluator=evaluator,
                       numFolds=5, seed=42)

cv_gbt = CrossValidator(estimator=pipeline_gbt,
                        estimatorParamMaps=paramGrid_gbt,
                        evaluator=evaluator,
                        numFolds=5, seed=42)

# Split data into training and test sets
train_data, test_data = df.randomSplit([0.7, 0.3], seed=42)

# Fit models with cross-validation on training set
cvModel_rf = cv_rf.fit(train_data)
cvModel_gbt = cv_gbt.fit(train_data)

# Get best models from each technique
best_rf = cvModel_rf.bestModel
best_gbt = cvModel_gbt.bestModel

# Apply best models to test data
pred_rf = best_rf.transform(test_data)
pred_gbt = best_gbt.transform(test_data)

# Evaluate RMSE and MAE
evaluator_rmse = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse")
evaluator_mae = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="mae")

rmse_rf = evaluator_rmse.evaluate(pred_rf)
mae_rf  = evaluator_mae.evaluate(pred_rf)
rmse_gbt = evaluator_rmse.evaluate(pred_gbt)
mae_gbt  = evaluator_mae.evaluate(pred_gbt)

# Reporting results
evaluation_timestamp = spark.sql("SELECT current_timestamp()").collect()[0][0]

report_data = [
    Row(model="Random Forest", rmse=rmse_rf, mae=mae_rf,
        best_model=None, evaluation_timestamp=evaluation_timestamp),
    Row(model="Gradient Boosted Trees", rmse=rmse_gbt, mae=mae_gbt,
        best_model=None, evaluation_timestamp=evaluation_timestamp),
    Row(model="Best Model", rmse=None, mae=None,
        best_model="Random Forest" if rmse_rf < rmse_gbt else "Gradient Boosted Trees",
        evaluation_timestamp=evaluation_timestamp)
]

report_df = spark.createDataFrame(report_data)

report_df.write.mode("overwrite") \
         .parquet("s3a://eafit-project-3-bucket/refined/models/results/model_evaluation/")

# Generate sintetic test data
start_date = datetime(2025, 3, 1)
dates = [start_date + timedelta(days=i) for i in range(60)]
    
data = {
    "date": [d.strftime("%Y-%m-%d") for d in dates],
    "id": list(range(1, 61)),
    "city": ["Medellín"] * 60,
    "max_temp": np.random.normal(loc=28, scale=1.5, size=60).round(1),
    "precipitation": np.random.uniform(0, 5, size=60).round(2),
    "congestion_level_num": np.random.randint(1, 7, size=60)
}
test_pd_df = pd.DataFrame(data)
    
# Store synthetic test data in S3
test_df = spark.createDataFrame(test_pd_df)
test_df.write.parquet("s3a://eafit-project-3-bucket/refined/models/test_data/", mode="overwrite")

# Load synthetic test data
predictions_df = spark.read.parquet("s3a://eafit-project-3-bucket/refined/models/test_data/*.parquet")

# Transform synthetic test data using the best models
predictions_rf = best_rf.transform(predictions_df)
predictions_gbt = best_gbt.transform(predictions_df)

# Select relevant columns and round predictions
result_rf = predictions_rf.select(
    "date", "id", "city", "max_temp", "precipitation", "congestion_level_num",
    round(col("prediction")).cast("int").alias("predicted_incidents")
)

result_gbt = predictions_gbt.select(
    "date", "id", "city", "max_temp", "precipitation", "congestion_level_num",
    round(col("prediction")).cast("int").alias("predicted_incidents")
)

# Save predictions to S3
result_rf.write.parquet("s3a://eafit-project-3-bucket/refined/models/results/predictions_rf/", mode="overwrite")
result_gbt.write.parquet("s3a://eafit-project-3-bucket/refined/models/results/predictions_gbt/", mode="overwrite")