def swap_author_title(sdf, spark):
    # TODO implement this in pyspark, don't use apply per row! (super slow and not scalable)
    # look at hello.ipynb and here:
    # https://stackoverflow.com/questions/43988801/pyspark-modify-column-values-when-another-column-value-satisfies-a-condition
    return sdf


def normalize_special_characters(sdf, spark):
    # TODO implement this
    str_columns = []
    for col in str_columns:
        # normalize column
        pass
    return sdf


def clean_year(sdf, spark):
    # TODO implement this
    return sdf


def clean_data(sdf, spark):
    sdf = normalize_special_characters(sdf, spark)
    sdf = clean_year(sdf, spark)
    sdf = swap_author_title(sdf, spark)
    return sdf
