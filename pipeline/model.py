from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.classification import GBTClassifier
import pyspark.sql.functions as f


features = [
    "pauthor_engineered",
    "peditor_engineered",
    "ptitle_engineered",
    "paddress_engineered",
    "ppublisher_engineered",
    "pseries_engineered",
    "pid_engineered",
    "partition_engineered",
    "book_title_full_engineered",
    "book_title_engineered",
    "journal_full_engineered",
    "journal_engineered",
    "source_type_engineered",
    "original_id_engineered",
]


def train_model(sdf):
    rf = GBTClassifier(labelCol="label", featuresCol="features")
    model = rf.fit(sdf)
    return model


def predict(model, sdf):
    return model.transform(sdf)


def evaluate(sdf):
    sdf = sdf.withColumn("hit", (sdf.label == sdf.prediction).cast("integer"))
    print(f"accuracy: {sdf.select(f.sum('hit')).collect()[0][0] / sdf.count()}")


# multi_evaluator = MulticlassClassificationEvaluator(
#     labelCol="Outcome", metricName="accuracy"
# )
# print("Random Forest classifier Accuracy:", multi_evaluator.evaluate(rf_predictions))
