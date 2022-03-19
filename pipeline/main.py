from spark import create_spark_session
from load_data import load_data, load_inputs, merge_inputs_with_data
from data_cleaning import clean_data
from feature_engineering import do_feature_engineering
from data_swapping import swap_data


def run_pipeline(input_path):
    print(" == running pipeline == ")
    spark = create_spark_session()
    print("loading data...")
    data = load_data(spark)
    inputs = load_inputs(input_path, spark)
    print("cleaning data...")
    data_cleaned = clean_data(data)
    print("swapping data...")
    data_swapped = swap_data(data_cleaned)    
    print("merging inputs with data...")
    inputs = merge_inputs_with_data(inputs, data_swapped)
    print("doing feature engineering...")
    inputs = do_feature_engineering(inputs, spark)
    return inputs


if __name__ == "__main__":
    inputs = run_pipeline("data/train.csv")
    print("writing data...")
    inputs.toPandas().set_index("input_id").to_csv("output/test_output.csv")

# TODO: train model and run on test inputs
# do this in a scalable way i. e. using pyspark (DO NOT CONVERT TO PANDAS OR NUMPY!)
# see for example:
# https://hackernoon.com/building-a-machine-learning-model-with-pyspark-a-step-by-step-guide-1z2d3ycd
# try a few different models
