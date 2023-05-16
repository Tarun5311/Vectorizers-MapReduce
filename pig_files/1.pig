-- Load the data from the .tsv file
data = LOAD '$input' USING PigStorage('\t') AS (filename:chararray, word:chararray, score:double);

-- Filter the data for a specific filename
filtered_data = FILTER data BY filename == '$filename';

-- Group the filtered data by filename
grouped_data = GROUP filtered_data BY filename;

-- Find the top N words with the highest scores for the specific filename
top_words = FOREACH grouped_data {
    ordered_data = ORDER filtered_data BY score DESC;
    top_n_words = LIMIT ordered_data $N;
    GENERATE FLATTEN(top_n_words);
}

-- Store the results into an output file
STORE top_words INTO '$output' USING PigStorage('\t');