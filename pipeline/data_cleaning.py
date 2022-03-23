import pyspark.sql.functions as f
# import re


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

# normalize special characters, make lowercase and trim excessive spaces
def normalize_special_characters(sdf, spark):
    str_columns = [    
    "peditor",
    "pauthor",
    "ptitle",
    "book_title_full", 
    "book_title", 
    "journal_full", 
    "journal",  
    "source_type", 
    ]
    for col_name in str_columns:
        sdf = sdf.withColumn(col_name, f.regexp_replace(f.encode(col_name, 'utf-8'), "[^a-zA-Z]", " ")) 
        sdf = sdf.withColumn(col_name, f.regexp_replace(col_name, "\s\s+", " "))
        sdf = sdf.withColumn(col_name, f.lower(f.col(col_name)))
        sdf = sdf.withColumn(col_name, f.trim(f.col(col_name)))
    return sdf

# make year absolute
def clean_year(sdf, spark):
    sdf = sdf.withColumn("pyear", f.abs(sdf.pyear))
    sdf = sdf.withColumn("pyear", f.ceil(sdf.pyear))
    return sdf

# transform source type into right format
def clean_source_type(sdf):
    sdf = sdf.withColumn("test_source", f.when(f.length(f.col("source_type")) <= 4, "book")
                .when(f.length(f.col("source_type")) <= 7, "article")
                .when(f.length(f.col("source_type")) <= 9, "phdthesis")
                .when(f.length(f.col("source_type")) <= 12, "incollection")
                .when(f.length(f.col("source_type")) <= 13, "inproceedings")
                .otherwise(f.col("source_type")))
    sdf = sdf.drop("source_type")
    sdf = sdf.withColumnRenamed("test_source", "source_type")
    return sdf

# function to call all data
def clean_data(sdf, spark):
    # sdf = normalize_special_characters(sdf, spark) # use this only if you'd like to check the accuracy when cleaning before swapping 
                                                    # (wouldn't recommend this because it'll cause problems during the swapping procedure)
    sdf = clean_year(sdf, spark)
    sdf = swap_author_title(sdf, spark)
    sdf = normalize_special_characters(sdf, spark) # use this to clean code after swapping
    sdf = clean_source_type(sdf)
    return sdf
