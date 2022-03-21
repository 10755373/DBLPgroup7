from spark import create_spark_session
from load_data import load_data, load_inputs, merge_inputs_with_data
from data_cleaning import clean_data
from feature_engineering import do_feature_engineering
from model import train_model, predict, evaluate


def run_pipeline(input_path):
    print(" == running pipeline == ")
    spark = create_spark_session()
    print("loading data...")
    data = load_data(spark)
    inputs = load_inputs(input_path, spark)
    print("cleaning data...")
    data = clean_data(data, spark)
    print("merging inputs with data...")
    inputs = merge_inputs_with_data(inputs, data)
    inputs.toPandas().to_csv("output/cleaned.csv")
    print("doing feature engineering...")
    inputs = do_feature_engineering(inputs, spark)
    return inputs


def prepare_submission():
    train = run_pipeline("data/train.csv")
    validation = run_pipeline("data/validation_hidden.csv")
    test = run_pipeline("data/test_hidden.csv")
    print("training model...")
    model = train_model(train)
    preds_v = predict(model, validation)
    preds_t = predict(model, test)
    print("writing data...")
    preds_v.toPandas().to_csv("output/val.csv")
    preds_t.toPandas().to_csv("output/test.csv")


def run_experiment():
    inputs = run_pipeline("data/train.csv")
    train, val = inputs.randomSplit([0.7, 0.3])
    model = train_model(train)
    preds = predict(model, val)
    evaluate(preds)  # this prints the accuracy (ctrl-f for accuracy in the log)


if __name__ == "__main__":
    run_experiment()
