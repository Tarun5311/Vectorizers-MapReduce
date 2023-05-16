import csv

def convert_vectorizer(file):
    words = []
    docs = []
    scores = []

    for line in file:
        doc, word, score = line.strip().split('\t')
        words.append(word)
        docs.append(doc)
        scores.append(score)

    return words, docs, scores
