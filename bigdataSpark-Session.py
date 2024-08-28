from pyspark.sql import SparkSession
from collections import Counter

# Start en SparkSession
spark = SparkSession.builder \
    .appName("WordCount") \
    .getOrCreate()

# Læs data fra filen (her antages det, at filen ligger på HDFS)
data = spark.read.text('hdfs:///path/to/AChristmasCarol_CharlesDickens_English.txt').rdd

# Split tekstlinjerne til ord
words = data.flatMap(lambda line: line[0].split())

# Tæl total antal ord
number_of_words = words.count()

# Tæl forekomsten af hvert ord
word_counts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

# Sorter ord efter frekvens i faldende orden
sorted_word_counts = word_counts.sortBy(lambda x: x[1], ascending=False)

# Skriv resultaterne til en tekstfil på HDFS
output_path = 'hdfs:///path/to/word_frequencies.txt'
sorted_word_counts.saveAsTextFile(output_path)

# Skriv det totale antal ord til en separat fil
with open('/path/to/local/number_of_words.txt', 'w') as output_file:
    output_file.write(f"Total number of words: {number_of_words}\n")

# Stop SparkSession
spark.stop()
