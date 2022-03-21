import pyspark.sql.functions as f


def swap_author_title(sdf, spark):
    # TODO implement this in pyspark, don't use apply per row! (super slow and not scalable)
    # look at hello.ipynb and here:
    # https://stackoverflow.com/questions/43988801/pyspark-modify-column-values-when-another-column-value-satisfies-a-condition
    def swap_cond(sdf):

        return (sdf.ptitle.contains("|")) | (
            ~sdf.ptitle.contains("|")
            & ~sdf.pauthor.contains("|")
            & (f.length("pauthor") > f.length("ptitle"))
        )

    sdf = sdf.withColumn(
        "new_author", f.when(swap_cond(sdf), sdf.ptitle).otherwise(sdf.pauthor)
    )
    sdf = sdf.withColumn(
        "new_title", f.when(swap_cond(sdf), sdf.pauthor).otherwise(sdf.ptitle)
    )
    sdf = sdf.drop("ptitle")
    sdf = sdf.drop("pauthor")
    sdf = sdf.withColumnRenamed("new_author", "pauthor")
    sdf = sdf.withColumnRenamed("new_title", "ptitle")
    return sdf


def normalize_special_characters(sdf, spark):
    # TODO implement this
    str_columns = []
    for col in str_columns:
        # normalize column
        pass
    return sdf


def clean_year(sdf, spark):
    return sdf.withColumn("pyear", f.abs(sdf.pyear))


def clean_data(sdf, spark):
    sdf = normalize_special_characters(sdf, spark)
    sdf = clean_year(sdf, spark)
    sdf = swap_author_title(sdf, spark)
    return sdf
