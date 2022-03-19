import pyspark.sql.functions as fn

# function to check whether author is correctly placed in pauthor column or not
def check_author(sdf):
    sdf = sdf.withColumn('wordCount_author', fn.size(fn.split(fn.col('pauthor'), ' ')))
    sdf = sdf.withColumn('aut1_bool', fn.when((sdf.pauthor.contains(r'|') | (fn.col('wordCount_author') < 5)), 1).otherwise(0))
    sdf = sdf.withColumn('aut_bool', fn.when((fn.col('aut1_bool') == 1), 1).otherwise(0))   
    sdf = sdf.drop('aut1_bool')
    return sdf

# swap author if needed
def swap_author(sdf):
    bc = sdf.withColumn('author', fn.when(fn.col('aut_bool') == 1, fn.col('pauthor')).otherwise(fn.col('ptitle')))
    return bc

# swap title if needed
def swap_title(sdf):
    bc = sdf.withColumn('title', fn.when(fn.col('aut_bool') == 1, fn.col('ptitle')).otherwise(fn.col('pauthor')))
    return bc

# function to call for swap functions
def swap_data(sdf):
    sdf = check_author(sdf)
    sdf = swap_author(sdf)
    sdf = swap_title(sdf)
    return sdf
