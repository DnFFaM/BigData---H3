from pyspark import SparkContext
from collections import Counter

# Start en SparkContext
sc = SparkContext("local", "WordCount")

# Læs data fra filen
data = sc.textFile('AChristmasCarol_CharlesDickens_English.txt')

# Split tekstlinjerne til ord
words = data.flatMap(lambda line: line.split())

# Tæl total antal ord
number_of_words = words.count()

# Tæl forekomsten af hvert ord
word_counts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

# Sorter ord efter frekvens i faldende orden
sorted_word_counts = word_counts.sortBy(lambda x: x[1], ascending=False).collect()

# Print det totale antal ord
print("Antal ord ialt: ", number_of_words)

# Print de sortere resultater til terminalen
for index, (word, count) in enumerate(sorted_word_counts, start=1):
    print(f"{index}. {word}: {count}")

# Skriv resultaterne til en tekstfil
with open('word_frequencies.txt', 'w') as output_file:
    output_file.write(f"Total number of words: {number_of_words}\n")
    output_file.write("Word Frequencies:\n")
    for index, (word, count) in enumerate(sorted_word_counts, start=1):
        output_file.write(f"{index}. {word}: {count}\n")

# Stop SparkContext
sc.stop()
