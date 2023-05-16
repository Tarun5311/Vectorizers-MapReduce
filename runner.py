import subprocess
from dotenv import load_dotenv
import os


def tfidf_run():
    load_dotenv()
    hadoop_home = "/home/tarunkumar/hadoop-3.3.1"
    tfidf_jar_path = "./jar_files/tfidf.jar"

    tfidf_command = [hadoop_home + "/bin/hadoop", "jar", tfidf_jar_path, "./input", "./df", "./tfidf"]
    subprocess.call(tfidf_command)


def bm25_run(num):
    load_dotenv()
    hadoop_home = "/home/tarunkumar/hadoop-3.3.1"
    bm25_jar_path = "./jar_files/BM25.jar"

    bm25_command = [hadoop_home + "/bin/hadoop", "jar", bm25_jar_path, "./input", "./df", "./dl", "./bm25", str(num)]
    subprocess.call(bm25_command)


def count_run():
    load_dotenv()
    hadoop_home = "/home/tarunkumar/hadoop-3.3.1"
    count_jar_path = "./jar_files/CountVectorizer.jar"

    count_command = [hadoop_home + "/bin/hadoop", "jar", count_jar_path, "./input", "./df", "./count"]
    subprocess.call(count_command)




def hashing_run(num=10000):
    load_dotenv()
    hadoop_home = "/home/tarunkumar/hadoop-3.3.1"
    hashing_jar_path = "./jar_files/HV.jar"

    hashing_command = [hadoop_home + "/bin/hadoop", "jar", hashing_jar_path, "./input", "./df", "./hashing", str(num)]
    subprocess.call(hashing_command)


def df_run(stopwords):
    load_dotenv()
    hadoop_home = "/home/tarunkumar/hadoop-3.3.1"

    if stopwords:
        df_jar_path = "./jar_files/DF.jar"
        df_command = [hadoop_home + "/bin/hadoop", "jar", df_jar_path, "./input", "./df", "./libraries/stopwords.txt"]
    else:
        df_jar_path = "./jar_files/DFWSWR.jar"
        df_command = [hadoop_home + "/bin/hadoop", "jar", df_jar_path, "./input", "./df"]
    subprocess.call(df_command)

def dl_run():
    load_dotenv()
    hadoop_home = "/home/tarunkumar/hadoop-3.3.1"
    dl_jar_path = "./jar_files/DL.jar"

    dl_command = [hadoop_home + "/bin/hadoop", "jar", dl_jar_path, "./input", "./dl"]
    subprocess.call(dl_command)



if __name__ == "__main__":
    bm25_run()
    
