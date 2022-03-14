from curses import def_prog_mode
from pyspark.sql.functions import * 
import nltk
from nltk.stem import WordNetLemmatizer
from nltk import word_tokenize 

from pyspark.ml.feature import Tokenizer, RegexTokenizer
from pyspark.sql.functions import col, udf
from pyspark.sql.types import IntegerType



nltk.download('punkt')
nltk.download('wordnet')

def swap_author_title(sdf):
    sdf = sdf.withColumn('wordCount_author', size(split(col('pauthor'), ' ')))
    sdf = sdf.select('*',
          when(check_swap(sdf.wordCount_author, sdf.pauthor) == 'author', [sdf.pauthor, sdf.ptitle])
          .when(check_swap(sdf.wordCount_author, sdf.pauthor) == 'title', [sdf.ptitle, sdf.pauthor])
          .otherwise(None).alias(['author_new', 'title_new'])
          .drop(['pauthor', 'ptitle', 'wordCount_author']))
    
    return sdf

def check_swap(wordCount_author, pauthor):
    # the or-condition isn't working, don't know why; will check again on Wednesday.
    if (array_contains(pauthor, r'|')) | (wordCount_author < 5):
        return 'author'
    else:
        return 'title'

def remove_some_chars(col_name):
    removed_chars = ("!", "?", '[', ']', '{', '}', '(', ')', '.', '*', '+', '=', '-', '_', "'", ':', ';', '<', '>', "''")
    regexp = "|".join('\{0}'.format(i) for i in removed_chars)
    return regexp_replace(col_name, regexp, "")


def cleanup(sdf):
    for col_name in ['pauthor', 'ptitle', 'journal_full', 'journal']:
        sdf = sdf.withColumn(col_name, remove_some_chars(col_name))
        sdf = sdf.withColumn(col_name, lower(col(col_name)))
        sdf = sdf.withColumn(col_name, trim(col(col_name)))
    return sdf


def clean_year(sdf):
    sdf = sdf.withColumn('pyear',ceil(abs(sdf.pyear)))
    return sdf


def clean_data(sdf):
    sdf = cleanup(sdf)
    sdf = clean_year(sdf)
    # sdf = tokenize_titles(sdf)
    # sdf = regextokenize_titles(sdf)
    # sdf = tokenize_author(sdf)
    # sdf = regextokenize_author(sdf)
    # sdf = swap_author_title(sdf)
    return sdf

# these functions I made when I was trying to lemmatize the words, will have a look again on Wednesday
# def tokenize_titles(sdf):
#     tokenizer = Tokenizer(inputCol="ptitle", outputCol="title_words")
#     tokenized = tokenizer.transform(sdf)
#     return tokenized

# def regextokenize_titles(sdf):
#     regexTokenizer = RegexTokenizer(inputCol="ptitle", outputCol="title_char", pattern="\\w")
#     tokenized = regexTokenizer.transform(sdf)
#     return tokenized

# def tokenize_author(sdf):
#     tokenizer = Tokenizer(inputCol="pauthor", outputCol="author_words")
#     tokenized = tokenizer.transform(sdf)
#     return tokenized

# def regextokenize_author(sdf):
#     regexTokenizer = RegexTokenizer(inputCol="pauthor", outputCol="author_char", pattern="\\w")
#     tokenized = regexTokenizer.transform(sdf)
#     return tokenized




