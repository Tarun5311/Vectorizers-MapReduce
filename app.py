import os
import random
import streamlit as st
import shutil
from runner import tfidf_run, df_run, dl_run
from convert import convert_vectorizer
from utils import removedir, word_docs
from vectorizer import tfidf_vectorizer, count_vectorizer, hashing_vectorizer, bm25_vectorizer, bm15_vectorizer, bm11_vectorizer
import plotly.graph_objs as go
import plotly.express as px
import pandas as pd
import altair as alt
from wordcloud import WordCloud
from pig import first, second
import matplotlib.pyplot as plt

st.set_page_config(layout="wide")


# variables
uploaded_files = []
uploaded_filenames = []


#functions
def file_exists(file_name):
    return file_name in uploaded_filenames

def save_uploaded_files():
    removedir("tfidf")
    removedir("bm25")
    removedir("count")
    removedir("hashing")
    removedir("bm11")
    removedir("bm15")
    for i, file in enumerate(uploaded_files):
        if file:
            with open(os.path.join("input", file.name), "wb") as f:
                f.write(file.getbuffer())

def compare_vec(option1, option2):
    print(option1)
    print(option2) 

def vectorizer_plot(option, plot_type):
    if option == "Count Vectorizer":
        words, docs, scores = count_vectorizer()
        get_plot(words, docs, scores, plot_type)
    elif option == 'TFIDF Vectorizer':
        words, docs, scores = tfidf_vectorizer()
        get_plot(words, docs, scores, plot_type)
    elif option == 'Hashing Vectorizer':
        features = st.number_input("Enter number of features")
        features = features if features > 0 else 1000
        features = int(features)
        words, docs, scores = hashing_vectorizer(features)
        get_plot(words, docs, scores, plot_type)
    elif option == "BM25 Vectorizer":
        words, docs, scores = bm25_vectorizer()
        get_plot(words, docs, scores, plot_type)
        removedir("bm25")
    elif option == "BM15 Vectorizer":
        words, docs, scores = bm15_vectorizer()
        get_plot(words, docs, scores, plot_type)
        removedir("bm25")
    elif option == "BM11 Vectorizer":
        words, docs, scores = bm11_vectorizer()
        get_plot(words, docs, scores, plot_type)
        removedir("bm25")
    else:
        print("buhahahah")

def get_data(option):
    words = []
    docs = []
    scores = []
    if option == "Count Vectorizer":
        words, docs, scores = count_vectorizer()
    elif option == 'TFIDF Vectorizer':
        words, docs, scores = tfidf_vectorizer()
    elif option == "BM25 Vectorizer":
        words, docs, scores = bm25_vectorizer()
        removedir("bm25")
    elif option == "BM15 Vectorizer":
        words, docs, scores = bm15_vectorizer()
        removedir("bm25")
    elif option == "BM11 Vectorizer":
        words, docs, scores = bm11_vectorizer()
        removedir("bm25")
    else:
        print("buhahahah")

    return words, docs, scores


def get_plot(words, docs, scores, plot):
    if plot == "bar":
        words_docs = word_docs(words, docs)
        fig = go.Figure(data=[go.Bar(x=words_docs, y=scores)])
        st.plotly_chart(fig, heigth=1000, width=2000, use_container_width=True)

    elif plot == "treemap":
        df = pd.DataFrame({
            'words': words,
            'docs': docs,
            'scores': scores
        })
        doc_colors = {doc: f"#{random.getrandbits(8*3):06x}" for doc in df['docs'].unique()}
        fig = px.treemap(
            df,
            path=['docs', 'words'],  # Specify the two levels
            values='scores',  # Specify the value to visualize
        )
        st.plotly_chart(fig, heigth=1000, width=2000, use_container_width=True)

    elif plot == "pie":
        words_docs = word_docs(words, docs)
        data = dict(
            words_docs=words_docs,
            scores=scores,
        )
        df = pd.DataFrame(data)
        fig = px.pie(df, values="scores", names="words_docs")
        st.plotly_chart(fig, heigth=1000, width=2000, use_container_width=True)

    elif plot == "altair":
        num = st.number_input("Enter Number of words")
        num = num if num > 0 else 20
        num = int(num)
        df = pd.DataFrame({
            'words': words,
            'docs': docs,
            'scores': scores
        })

        def plot_top_words(df):
            docs = df['docs'].unique().tolist()
            plots = []
            for doc in docs:
                # Get the top 20 words based on their scores for the current doc
                top_words = df[df['docs']==doc].sort_values('scores', ascending=False).head(num)
                # Create a chart for the current doc
                chart = alt.Chart(top_words).mark_bar().encode(
                    x=alt.X('scores', axis=alt.Axis(title='Scores')),
                    y=alt.Y('words', sort=alt.EncodingSortField(field='scores', order='descending'), axis=alt.Axis(title=f'Top {num} Words')),
                    color='words'
                ).properties(
                    title=f'Top {num} Words in {doc}'
                )
                # Add the chart to the list of plots
                plots.append(chart)
            # Combine all the charts into a single chart
            chart = alt.hconcat(*plots)
            return chart

        st.altair_chart(plot_top_words(df), use_container_width=True)

    elif plot == 'wordcloud':
        data = dict(
            words=words,
            docs=docs,
            scores=scores,
        )
        data = pd.DataFrame(data)
        docs = data['docs'].unique().tolist()

        selected_doc = st.selectbox('Select a document:', docs)
        df = data[data['docs'] == selected_doc]

        word_freq = dict(zip(df['words'], df['scores'].astype(float)))
        # print(type(df['scores'][0]))
        wordcloud = WordCloud(width=400, height=200, background_color='white', colormap='Blues', max_words=200).generate_from_frequencies(word_freq)

        st.image(wordcloud.to_image(), use_column_width=True)
    


#Starting
if not os.path.exists("input"):
    os.makedirs("input")

for file_name in os.listdir("input"):
    file_path = os.path.join("input", file_name)
    if os.path.isfile(file_path):
        os.remove(file_path)

st.title("Vectorizer Visualization")
dropped_files = st.file_uploader("Drop your files here", type="txt", accept_multiple_files=True, key="fileUploader")

if dropped_files:
    for file in dropped_files:
        if not file_exists(file.name):
            uploaded_files.append(file)
            uploaded_filenames.append(file.name)
        else:
            st.write(f"{file.name} already exists and cannot be uploaded again")
    save_uploaded_files()

col1, col2 = st.columns(2)
with col1:
    stopwords = st.checkbox("Stop Words")
with col2:
    enter = st.button("Enter")
    

if enter:
    if stopwords:
        removedir("df")
        removedir("dl")
        df_run(True)
        dl_run()
    else:
        removedir("df")
        removedir("dl")
        df_run(False)
        dl_run()


st.subheader("View")
option = st.selectbox(
    'Select the type of vectorizer',
    (
        'None',
        'Count Vectorizer',
        'Hashing Vectorizer',
        'TFIDF Vectorizer',
        'BM25 Vectorizer',
        'BM15 Vectorizer',
        'BM11 Vectorizer'
    )
)

plot_type = st.selectbox(
    'Select the type of plot',
    (
        'bar',
        'treemap',
        'pie',
        'altair',
        'wordcloud'
    )
)

vectorizer_plot(option, plot_type)

st.subheader("Compare")
col1, col2= st.columns([3,3])

with col1:
    option1 = st.selectbox(
        'Select First vectorizer',
        (
            'None',
            'Count Vectorizer',
            'TFIDF Vectorizer',
            'BM25 Vectorizer',
            'BM15 Vectorizer',
            'BM11 Vectorizer'
        )
    )
with col2:
    option2 = st.selectbox(
        'Select Second vectorizer',
        (
            'None',
            'Count Vectorizer',
            'TFIDF Vectorizer',
            'BM25 Vectorizer',
            'BM15 Vectorizer',
            'BM11 Vectorizer'
        )
    )
plot_type1 = st.selectbox(
    'Select the type of plot for comparision',
    (
        'bar',
        'treemap',
        'pie',
        'altair'
    )
)
st.button("Compare", on_click=compare_vec, args=(option1, option2))


vectorizer_plot(option1, plot_type1)
vectorizer_plot(option2, plot_type1)


st.subheader("Analytics")
option3 = st.selectbox(
    'Select a Query',
    (
        'Top N words from a document for a vectorizer',
        'Get average scores for each word for a vectorizer'
    )
)

if option3 == 'Top N words from a document for a vectorizer':
    col1, col2, col3 = st.columns([2,2,2])
    words = []
    docs = []
    scores = []
    with col1:
        vectorizer = st.selectbox(
            'Select Vectorizer',
            (
                'None',
                'Count Vectorizer',
                'TFIDF Vectorizer',
                'BM25 Vectorizer',
                'BM15 Vectorizer',
                'BM11 Vectorizer'
            )
        )
    with col2:
        n = st.number_input('Enter Number of words')
        n = int(n)
        n = n if n > 0 else 5
    with col3:
        doc = st.selectbox(
            'Select document',
            os.listdir("input")
        )
    print("heloooooooooo")
    with st.spinner('Processing...'):
        first(vectorizer, n, doc)
        if os.path.exists("output"):
            # print("ouputttttttttttttttttt")
            os.rename("output/part-r-00000", "output.tsv")
            df = pd.read_csv('output.tsv', sep='\t', names=['docs', 'words', 'scores'])
            doc_rows = df[df['docs'] == doc]
            removedir('output')
            os.remove('output.tsv')
            fig, ax = plt.subplots()
            fig = px.line(x=df['words'].to_list(), y=df['scores'].to_list(), title='Line Chart')
            st.plotly_chart(fig)


elif option3 == 'Get average scores for each word for a vectorizer':
    vectorizer = st.selectbox(
        'Select Vectorizer',
        (
            'Count Vectorizer',
            'TFIDF Vectorizer',
            'BM25 Vectorizer',
            'BM15 Vectorizer',
            'BM11 Vectorizer'
        )
    ) 
    
    with st.spinner('Processing...'):
        second(vectorizer)
        if os.path.exists("output"):
            os.rename("output/part-r-00000", "output.tsv")
            df = pd.read_csv('output.tsv', sep='\t', names=['words', 'scores'])
            removedir('output')
            os.remove('output.tsv')
            fig = px.line(x=df['words'].to_list(), y=df['scores'].to_list(), title='Line Chart')
            st.plotly_chart(fig)






