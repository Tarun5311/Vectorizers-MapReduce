import os
import streamlit as st
import shutil
from runner import tfidf_run, df_run, dl_run, count_run, hashing_run, bm25_run
from convert import convert_vectorizer

def tfidf_vectorizer():
    words = []
    docs = []
    scores = []
    spinner = st.spinner('Processing...')
    with st.spinner('Processing...'):
        if os.path.exists("tfidf"):
            with open("tfidf/TFIDF.tsv", "r") as f:
                words, docs, scores = convert_vectorizer(f)
        else:
            tfidf_run()
            with open("tfidf/TFIDF.tsv", "r") as f:
                words, docs, scores = convert_vectorizer(f)
    return words, docs, scores


def count_vectorizer():
    words = []
    docs = []
    scores = []
    spinner = st.spinner('Processing...')
    with st.spinner('Processing...'):
        if os.path.exists("count"):
            with open("count/CV.tsv", "r") as f:
                words, docs, scores = convert_vectorizer(f)
        else:
            count_run()
            with open("count/CV.tsv", "r") as f:
                words, docs, scores = convert_vectorizer(f)
    return words, docs, scores


def hashing_vectorizer(num):
    words = []
    docs = []
    scores = []
    spinner = st.spinner('Processing...')
    with st.spinner('Processing...'):
        if os.path.exists("hashing"):
            with open("hashing/HV.tsv", "r") as f:
                words, docs, scores = convert_vectorizer(f)
        else:
            hashing_run(num)
            with open("hashing/HV.tsv", "r") as f:
                words, docs, scores = convert_vectorizer(f)
    return words, docs, scores


def bm25_vectorizer(num=0.75):
    words = []
    docs = []
    scores = []
    spinner = st.spinner('Processing...')
    with st.spinner('Processing...'):
        if os.path.exists("bm25"):
            with open("bm25/BM25.tsv", "r") as f:
                words, docs, scores = convert_vectorizer(f)
        else:
            bm25_run(num)
            with open("bm25/BM25.tsv", "r") as f:
                words, docs, scores = convert_vectorizer(f)
    return words, docs, scores


def bm15_vectorizer():
    words, docs, scores = bm25_vectorizer(0)
    return words, docs, scores

def bm11_vectorizer():
    words, docs, scores = bm25_vectorizer(1)
    return words, docs, scores