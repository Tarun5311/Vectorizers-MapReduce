import subprocess
import os
import streamlit as st
from runner import tfidf_run, df_run, dl_run, count_run, hashing_run, bm25_run
from utils import removedir

def get_input(option):
    input_str=""
    if option == "Count Vectorizer":
        with st.spinner('Processing...'):
            if not os.path.exists("count"):
                count_run()
            input_str="./count/CV.tsv"  
            return input_str
    elif option == 'TFIDF Vectorizer':
         with st.spinner('Processing...'):
            if not os.path.exists("tfidf"):
                tfidf_run()
            input_str="./tfidf/TFIDF.tsv"  
            return input_str
    elif option == "BM25 Vectorizer":
        with st.spinner('Processing...'):
            if not os.path.exists("bm25"):
                bm25_run(0.75)
            input_str="./bm25/BM25.tsv"  
            removedir("bm25")
            return input_str
    elif option == "BM15 Vectorizer":
        with st.spinner('Processing...'):
            if not os.path.exists("bm25"):
                bm25_run(0)
            input_str="./bm25/BM25.tsv"  
            removedir("bm25")
            return input_str
    elif option == "BM11 Vectorizer":
        with st.spinner('Processing...'):
            if not os.path.exists("bm25"):
                bm25_run(1)
            input_str="./bm25/BM25.tsv"  
            removedir("bm25")
            return input_str
    else:
        print("buhahahah")

# Build the Pig command
def first(vectorizer, N, filename):

    if vectorizer is not "None":
        input=get_input(vectorizer)

        command = [
            "pig","-param","input="+input,"-param","filename="+filename,"-param","N="+str(N), "-param", "output="+"./output", "-f", "./pig_files/1.pig"
        ]

        try:
            # Run the Pig command
            subprocess.check_output(command)
            print("Pig script executed successfully.")
        except subprocess.CalledProcessError as e:
            print("Error while running Pig script:", e)


def second(vectorizer):
    if vectorizer is not "None":
        input=get_input(vectorizer)
        command = [
            "pig","-param","input="+input, "-param", "output="+"./output", "-f", "./pig_files/2.pig"
        ]

        try:
            # Run the Pig command
            subprocess.check_output(command)
            print("Pig script executed successfully.")
        except subprocess.CalledProcessError as e:
            print("Error while running Pig script:", e)


