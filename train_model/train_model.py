"""
Training script using Spark to read data from HDFS, train Random Forest,
evaluate, and save the model back to HDFS.
"""

import sys


# =========================
# 1. Import libraries
# =========================
import pandas as pd

from sklearn.metrics import (
    classification_report,
    confusion_matrix,
    roc_auc_score,
)

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier as SparkRandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator


# =========================
# 2. Create Spark session
# =========================
print("[STEP 1] Creating Spark session...", file=sys.stdout)
spark = SparkSession.builder \
    .appName("FraudDetectionTrainingSpark") \
    .getOrCreate()
print("[STEP 1] Spark session created.", file=sys.stdout)

spark.sparkContext.setLogLevel("ERROR")
# =========================
# 3. Load data from HDFS
# =========================
print("[STEP 2] Loading data from HDFS...", file=sys.stdout)
data_path = "hdfs://192.168.80.52:9000/data/train.csv"

df_spark = spark.read.csv(
    data_path,
    header=True,
    inferSchema=True
)

print("[STEP 2] Data loaded from HDFS.", file=sys.stdout)
print("  -> Dataset count:", df_spark.count())
print("  -> Class distribution (full dataset):")
df_spark.groupBy("Class").count().show()


# =========================
# 4. Prepare features & label
# =========================
print("[STEP 3] Preparing features and label...", file=sys.stdout)
feature_cols = [c for c in df_spark.columns if c != "Class"]

assembler = VectorAssembler(
    inputCols=feature_cols,
    outputCol="features"
)

df_assembled = assembler.transform(df_spark)

df_final = df_assembled.select("features", "Class") \
    .withColumnRenamed("Class", "label")
print("[STEP 3] Features and label prepared.", file=sys.stdout)


# =========================
# 5. Train / Test split
# =========================
print("[STEP 4] Splitting data into train and test sets...", file=sys.stdout)
train_df, test_df = df_final.randomSplit([0.8, 0.2], seed=42)

print("[STEP 4] Train/Test split done.", file=sys.stdout)
print("  -> Label distribution in train set:")
train_df.groupBy("label").count().show()


# =========================
# 6. Train Random Forest with Spark
# =========================
print("[STEP 5] Training Random Forest model with Spark...", file=sys.stdout)
rf = SparkRandomForestClassifier(
    numTrees=300,
    maxDepth=15,
    labelCol="label",
    featuresCol="features",
    seed=42
)

model = rf.fit(train_df)
print("[STEP 5] Model training completed.", file=sys.stdout)


# =========================
# 7. Run predictions on test set
# =========================
print("[STEP 6] Running predictions on test set...", file=sys.stdout)
predictions = model.transform(test_df)
print("[STEP 6] Predictions completed.", file=sys.stdout)


# =========================
# 8. Evaluation
# =========================
print("[STEP 7] Evaluating model...", file=sys.stdout)

# 8.1 ROC-AUC using Spark evaluator (uses rawPrediction/probability)
evaluator = BinaryClassificationEvaluator(
    labelCol="label",
    rawPredictionCol="rawPrediction",
    metricName="areaUnderROC",
)
roc_auc = evaluator.evaluate(predictions)

# 8.2 Confusion matrix & classification report (collect to Pandas)
pred_pd = predictions.select("label", "prediction").toPandas()

y_test = pred_pd["label"]
y_pred = pred_pd["prediction"]

print("\nConfusion Matrix:")
print(confusion_matrix(y_test, y_pred))

print("\nClassification Report:")
print(classification_report(y_test, y_pred))

print("\nROC-AUC (Spark evaluator):", roc_auc)
print("[STEP 7] Evaluation finished.", file=sys.stdout)


# =========================
# 10. Save model to HDFS
# =========================
print("[STEP 8] Saving model to HDFS...", file=sys.stdout)
model_output_path = "hdfs://192.168.80.52:9000/model"
model.write().overwrite().save(model_output_path)

print(f"\nModel saved to: {model_output_path}")
print("[STEP 8] Model save completed.", file=sys.stdout)


# =========================
# 11. Stop Spark session
# =========================
print("[STEP 9] Stopping Spark session...", file=sys.stdout)
spark.stop()
print("[STEP 9] Spark session stopped. Training pipeline finished.", file=sys.stdout)