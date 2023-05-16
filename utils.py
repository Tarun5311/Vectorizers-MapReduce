import os
import shutil

def removedir(dir):
    if os.path.exists(dir):
        shutil.rmtree(dir)

def word_docs(words, docs):
    words_docs = []
    for i in range(len(words)):
        words_docs.append(f"{words[i]} - {docs[i]}")
    
    return words_docs
    