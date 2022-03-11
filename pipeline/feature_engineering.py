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
import pyspark.sql.functions as f

DATA_COLUMNS = [
    "pauthor",
    "peditor",
    "ptitle",
    "pyear",
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


def do_feature_engineering(sdf, spark):
    for col in DATA_COLUMNS:
        sdf = one_hot_encode_equal(sdf, col)
    return sdf
