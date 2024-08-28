from collections import Counter

number_of_words = 0

# Open the file and read its contents
with open(r'AChristmasCarol_CharlesDickens_English.txt', 'r') as file:
    data = file.read()

# Split the text into words
words = data.split()

# Count the total number of words
number_of_words = len(words)

# Print total number of words
print("Antal ord ialt: ", number_of_words)

# Count the frequency of each word
word_counts = Counter(words)

# Sort words by frequency in descending order
sorted_word_counts = sorted(word_counts.items(), key=lambda x: x[1], reverse=True)

# Print the results to the terminal
for index, (word, count) in enumerate(sorted_word_counts, start=1):
    print(f"{index}. {word}: {count}")

# Write the results to a text file
with open('word_frequencies.txt', 'w') as output_file:
    output_file.write(f"Total number of words: {number_of_words}\n")
    output_file.write("Word Frequencies:\n")
    for index, (word, count) in enumerate(sorted_word_counts, start=1):
        output_file.write(f"{index}. {word}: {count}\n")
