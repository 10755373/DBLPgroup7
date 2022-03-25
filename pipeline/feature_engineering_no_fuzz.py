"""
Feature engineering without FuzzyWuzzy
"""
from lib2to3.pytree import convert
import pyspark.sql.functions as f
from pyspark.ml.feature import VectorAssembler
# from fuzzywuzzy import fuzz
from pyspark.sql.types import StringType, FloatType, IntegerType



DEAFAULT_ENGINNERING_COLS = [
    "peditor",
    "paddress",
    "ppublisher",
    "pseries",
    "pid",
    "partition",
    "book_title_full",
    "book_title",
    "journal_full",
    "journal",
    "source_type",
    "original_id",
]

ALL_ENGINEERED_COLS = [
    "key_engineered",
    "ptitle_engineered",
    "pauthor_engineered",
    "peditor_engineered",
    "ptitle_engineered",
    "paddress_engineered",
    "ppublisher_engineered",
    "pseries_engineered",
    "pid_engineered",
    "book_title_full_engineered",
    "book_title_engineered",
    "journal_full_engineered",
    "journal_engineered",
    "source_type_engineered",
    "original_id_engineered",
    "pyear_engineered",
]


def one_hot_encode_equal(sdf, col):
    """
    One hot encode wheter values agree for two inputs.
    """
    col_x, col_y = col + "_x", col + "_y"
    sdf = sdf.withColumn(
        col + "_engineered", (f.col(col_x) == f.col(col_y)).cast("integer")
    )
    sdf = sdf.drop(col_x, col_y)
    return sdf


def engineer_year(sdf):
    sdf = sdf.withColumn("pyear_engineered", f.abs(f.col("pyear_x") - f.col("pyear_y")))
    sdf = sdf.drop("pyear_x", "pyear_y")
    return sdf

# based on this site: https://stackoverflow.com/questions/69348997/similarity-between-two-strings-in-a-pyspark-dataframe
def enginner_author(sdf):
    sdf = sdf \
            .withColumn("pauthor_engineered",f.concat_ws("/",f.size(f.array_intersect(f.split(f.col("pauthor_x")," "),f.split(f.col("pauthor_y")," "))),f.size(f.split(f.col("pauthor_y")," "))))\
            .drop("pauthor_x", "pauthor_y")
            # .select(f.col("pauthor_engineered").cast(IntegerType()))\
            # .drop("pauthor_x", "pauthor_y")
    print("author_engineered worked!")
    return sdf


def engineer_key(sdf):
    sdf = sdf \
        .withColumn("key_engineered",f.concat_ws("/",f.size(f.array_intersect(f.split(f.col("key1")," "),f.split(f.col("key2")," "))),f.size(f.split(f.col("key2")," "))))
        # .select(f.col("key_engineered").cast(IntegerType()))
    print("key_engineered worked!")
    return sdf


def engineer_title(sdf):
    sdf = sdf \
            .withColumn("ptitle_engineered",f.concat_ws("/",f.size(f.array_intersect(f.split(f.col("ptitle_x")," "),f.split(f.col("ptitle_y")," "))),f.size(f.split(f.col("ptitle_y")," "))))\
            .drop("ptitle_x", "ptitle_y")
            # .select(f.col("ptitle_engineered").cast(IntegerType()))\
            # .drop("ptitle_x", "ptitle_y")
    print("title_engineered worked!")
    return sdf


def vectorize_features(sdf):
    assembler = VectorAssembler(
        inputCols=ALL_ENGINEERED_COLS, outputCol="features", handleInvalid="skip"
    )
    sdf = assembler.transform(sdf)
    return sdf


def convert_label(sdf):
    if "label" in sdf.columns:
        sdf = sdf.withColumn("label", (f.col("label") == "True").cast("integer"))

    return sdf


def deal_with_nans(sdf):
    cols = list(ALL_ENGINEERED_COLS)
    cols.remove("pyear_engineered")
    sdf = sdf.replace(0, -1, cols)
    sdf = sdf.fillna(0)
    return sdf

# calculate similarity solely based on the levehstein distance (so only after cleaning the code, without using FuzzyWuzzy)
def engineer_aut_key_title(sdf):
    sdf = sdf \
                .withColumn("pauthor_engineered",f.concat_ws("/",f.size(f.array_intersect(f.split(f.col("pauthor_x")," "),f.split(f.col("pauthor_y")," "))),f.size(f.split(f.col("pauthor_y")," "))))\
                .withColumn("key_engineered",f.concat_ws("/",f.size(f.array_intersect(f.split(f.col("key1")," "),f.split(f.col("key2")," "))),f.size(f.split(f.col("key2")," "))))\
                .withColumn("ptitle_engineered",f.concat_ws("/",f.size(f.array_intersect(f.split(f.col("ptitle_x")," "),f.split(f.col("ptitle_y")," "))),f.size(f.split(f.col("ptitle_y")," "))))\
                .drop("pauthor_x", "pauthor_y")\
                .drop("ptitle_x", "ptitle_y")
    cols = list(["pauthor_engineered", "key_engineered", "ptitle_engineered"])
    # sdf.select(
    # *[f.col(col).cast(FloatType()).alias(col) for col in cols]
    # )
    for col_name in cols:
        sdf = sdf.withColumn(col_name, f.col(col_name).cast('float'))
    return sdf


def do_feature_engineering(sdf, spark):
    for col in DEAFAULT_ENGINNERING_COLS:
        sdf = one_hot_encode_equal(sdf, col)
    sdf = engineer_year(sdf)
    # sdf = enginner_author(sdf)
    # sdf = engineer_title(sdf)
    # sdf = engineer_key(sdf)
    sdf = engineer_aut_key_title(sdf)
    sdf = convert_label(sdf)
    sdf = deal_with_nans(sdf)
    sdf = vectorize_features(sdf)
    return sdf

