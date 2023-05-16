-- Load the data from the .tsv file
data = LOAD '$input' USING PigStorage('\t') AS (filename:chararray, word:chararray, score:double);

-- Group the data by word
grouped_data = GROUP data BY word;

-- Calculate the average score for each word
average_scores = FOREACH grouped_data {
    total_score = SUM(data.score);
    num_files = COUNT(data);
    average_score = total_score / num_files;
    GENERATE group AS word, average_score;
}

-- Store the results into an output file
STORE average_scores INTO '$output' USING PigStorage('\t');