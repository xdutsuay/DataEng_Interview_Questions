from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
import requests
import re

# Initialize SparkSession
spark = SparkSession.builder.appName("ScrabbleScore").getOrCreate()


# Function to calculate scrabble score for a word
def calculate_scrabble_score(word):
    scrabble_scores = {'A': 1, 'B': 3, 'C': 3, 'D': 2, 'E': 1, 'F': 4, 'G': 2, 'H': 4,
                       'I': 1, 'J': 8, 'K': 5, 'L': 1, 'M': 3, 'N': 1, 'O': 1, 'P': 3,
                       'Q': 10, 'R': 1, 'S': 1, 'T': 1, 'U': 1, 'V': 4, 'W': 4, 'X': 8,
                       'Y': 4, 'Z': 10}

    return sum(scrabble_scores.get(char.upper(), 0) for char in word)


# Function to fetch text from a URL
def get_text_from_url(url):
    response = requests.get(url)
    return response.text


# Define a UDF for calculating scrabble score
calculate_scrabble_score_udf = udf(calculate_scrabble_score, IntegerType())

# URL of the complete works of Shakespeare
shakespeare_url = 'https://www.gutenberg.org/cache/epub/100/pg100.txt'

# Fetch text from the URL
shakespeare_text = get_text_from_url(shakespeare_url)

# Create a DataFrame with a single column 'text'
df = spark.createDataFrame([(shakespeare_text,)], ["text"])

# Remove non-alphabetic characters and convert to uppercase
df_clean = df.withColumn("clean_text", udf(lambda x: re.sub(r'[^A-Za-z]+', '', x).upper())("text"))

# Calculate scrabble score for each word
df_scores = df_clean.withColumn("scores",
                                udf(lambda x: sum(calculate_scrabble_score(char) for char in x.split()), IntegerType())(
                                    "clean_text"))

# Calculate and print the total scrabble score
total_score = df_scores.agg({"scores": "sum"}).collect()[0][0]
print(f'The total scrabble score for the complete works of Shakespeare is: {total_score}')

# Stop the SparkSession
spark.stop()
