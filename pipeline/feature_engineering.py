"""
The goal of this module is to engineer a feature from pairs of values in the same column from two records. 
At the end we should get rid of all the columns with suffixes '_x' and '_y' 
and replace them with numeric values (as most simple models require numeric features only).

Before designing a new engineering function for a column take a look at the data and determine how 
it can be usefully encoded in a way that correlates with the duplication label.

For now we just one hot encode whether two values are equal, but this should be improved.
Consider fuzzy matching for titles and authors. 
In case of year maybe take the difference in years.
etc.
"""
from lib2to3.pytree import convert
import pyspark.sql.functions as f
from pyspark.ml.feature import VectorAssembler
from fuzzywuzzy import fuzz
from pyspark.sql.types import IntegerType

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


def enginner_author(sdf):
    ratio = f.udf(fuzz.token_set_ratio, IntegerType())
    sdf = sdf.withColumn("pauthor_engineered", ratio("pauthor_x", "pauthor_y"))
    sdf = sdf.drop("pauthor_x", "pauthor_y")
    return sdf


def engineer_key(sdf):
    ratio = f.udf(fuzz.token_set_ratio, IntegerType())
    sdf = sdf.withColumn("key_engineered", ratio("key1", "key2"))
    return sdf


def engineer_title(sdf):
    ratio = f.udf(fuzz.token_set_ratio, IntegerType())
    sdf = sdf.withColumn("ptitle_engineered", ratio("ptitle_x", "ptitle_y"))
    sdf = sdf.drop("ptitle_x", "ptitle_y")
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


def do_feature_engineering(sdf, spark):
    for col in DEAFAULT_ENGINNERING_COLS:
        sdf = one_hot_encode_equal(sdf, col)
    sdf = engineer_year(sdf)
    sdf = enginner_author(sdf)
    sdf = engineer_title(sdf)
    sdf = engineer_key(sdf)
    sdf = convert_label(sdf)
    sdf = deal_with_nans(sdf)
    sdf = vectorize_features(sdf)
    return sdf
