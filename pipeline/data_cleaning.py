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
    for col in str_columns:
        # sdf = sdf.withColumn(col, f.regexp_replace(col, "[^0-9a-zA-Z]+", " "))    
        # sdf = sdf.withColumn(col, f.regexp_replace(col, "[^0-9a-zA-Z]+", " "))
        # sdf = sdf.withColumn(col, f.regexp_replace(f.encode(col, 'utf-8'), "[^0-9a-zA-Z]+", " "))
        sdf = sdf.withColumn(col + "_cleaned", f.regexp_replace(f.encode(col, 'utf-8'), "[^a-zA-Z]", " ")) 
        # sdf = sdf.withColumn(col + "_cleaned" + "_new", re.sub("\s\s+", " ", col))       
        # # '^[a-zA-Z]+$' | [^0-9a-zA-Z]+ | [^a-zA-Z]
        # sdf = sdf.drop(col)  
        # spark.sql(""" SELECT regexp_replace(decode(encode('Ä??ABCDE', 'utf-8'), 'ascii'), "[^\t\n\r\x20-\x7F]","")  x """).show(false)
        sdf = sdf.drop(col)
        sdf = sdf.withColumnRenamed(col + "_cleaned", col)
        sdf = sdf.withColumn(col + "_cleaned_new", f.regexp_replace(col, "\s\s+", " "))
        sdf = sdf.drop(col)
        sdf = sdf.withColumnRenamed(col + "_cleaned_new", col)
    return sdf




def clean_year(sdf, spark):
    return sdf.withColumn("pyear", f.abs(sdf.pyear))


def clean_data(sdf, spark):
    # sdf = normalize_special_characters(sdf, spark) # use this only if you'd like to check the accuracy when cleaning before swapping 
                                                    # (wouldn't recommend this because it'll cause problems during the swapping procedure)
    sdf = clean_year(sdf, spark)
    sdf = swap_author_title(sdf, spark)
    sdf = normalize_special_characters(sdf, spark) # use this to clean code after swapping
    return sdf
