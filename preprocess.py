import re
import nltk
from nltk.stem import WordNetLemmatizer

nltk.download('punkt')
nltk.download('wordnet')

def string_cleaner(string):
    # lowercase string
    string = string.lower()  
    # remove references
    string = re.sub('[\(\[].*?[\)\]]', '', string)  
    # remove 's
    string = re.sub("'s", ' ', string)  
    # remove '
    string = re.sub("'", '', string)  
    # remove -
    string = re.sub('-', ' ', string) 
    # remove quotations
    string = re.sub("\'", '', string)  
    # remove punctuations
    string = re.sub(r'[^a-zA-Z0-9 ]', '', string)  
    # remove >2 spaces
    string = re.sub(r'  +', ' ', string)  
    # remove spaces
    string = string.strip()  
    return string

def lemmatize(tokens):
    lem = WordNetLemmatizer()  # instantiate model
    return [lem.lemmatize(token) for token in tokens]

    