# Question: Given a large text dataset (e.g., the complete works of Shakespeare), the task is to calculate the total
# Scrabble tile score of all the letters in the dataset. Scrabble scores should be calculated for each word,
# excluding punctuation, spaces, and numbers. The function `calculate_scrabble_score` should be utilized,
# which assigns scores based on individual letter scores. The final result should be the sum of scores for all words
# in the dataset. Can we efficiently achieve this using PySpark for distributed processing?


import requests
import re

# Function to calculate Scrabble score for a word
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

# Function to calculate scrabble score for a given text
def calculate_total_scrabble_score(text):
    # Remove non-alphabetic characters and convert to uppercase
    clean_text = re.sub(r'[^A-Za-z]+', '', text).upper()
    
    # Calculate scrabble score for each word
    scores = [calculate_scrabble_score(word) for word in clean_text.split()]

    # Return the total score
    return sum(scores)

# URL of the complete works of Shakespeare
shakespeare_url = 'https://www.gutenberg.org/cache/epub/100/pg100.txt'

# Fetch text from the URL
shakespeare_text = get_text_from_url(shakespeare_url)

# Calculate and print the total Scrabble score
total_score = calculate_total_scrabble_score(shakespeare_text)
print(f'The total scrabble score for the complete works of Shakespeare is: {total_score}')
