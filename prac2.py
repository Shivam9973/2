import csv
import re
import nltk
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
nltk.download('stopwords')
nltk.download('wordnet')
def clean_text(text):
    # Remove unwanted characters and emojis
    cleaned_text = re.sub(r'[^\w\s]', '', text)
    cleaned_text = re.sub(r'[^\x00-\x7F]+', '', cleaned_text)
    return cleaned_text.strip()
def lemmatize_text(text):
    lemmatizer = WordNetLemmatizer()
    lemmatized_words = [lemmatizer.lemmatize(word) for word in text.split()]
    return ' '.join(lemmatized_words)
# Path to the CSV file
csv_file = 'C:\\Users\\student\\Desktop\\train.csv'
# List to store cleaned reviews
cleaned_reviews = []
# Read the CSV file and clean the data
with open(csv_file, 'r', encoding='utf-8') as file:
    reader = csv.reader(file)
    header = next(reader) # Skip the header row
    for row in reader:
        review = row[0]
        cleaned_review = clean_text(review)
        lemmatized_review = lemmatize_text(cleaned_review)
        cleaned_reviews.append([lemmatized_review])
# Write the cleaned data back to the CSV file
with open(csv_file, 'w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(header)
    writer.writerows(cleaned_reviews)
print("Data cleaning and lemmatization completed!")
# in terminal docker-hadoop open command>docker-compose up -d
# docker exec -it namenode /bin/bash
# root:/# cd tmp
# :/tmp# mkdir message
#:/tmp# exit
# command:-docker container ls
# docker cp C:\path of your dataset your container ID of namenode:/tmp/message
# successfully copied to given container id:/tmp/message
# docker exec -it namenode /bin/bash
# root…….. :/# cd tmp
# root…….. :/tmp# hadoop fs -mkdir -p messageFile
# root…….. :/tmp# hdfs dfs -put ./message/* messageFile
# 2023-10-01 08:36:51,512 INFO sasl.SaslDataTransferClient: SASL encryption trust check: localHostTrusted = false, remoteHostTrusted = false
# root…….. :/# cd /tmp
# root…….. :/tmp# hadoop jar hadoop-mapreduce-examples-2.7.1-sources.jar org.apache.hadoop.examples.WordCount messageFile finalOutput
# root…….. :/tmp# hdfs dfs -ls -R OR root…….. :/tmp# hdfs dfs -ls
# root…….. :/tmp# OR root…….. :/tmp# hdfs dfs -cat finalOutput/part-r-00000
