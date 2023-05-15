package Problem2;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import opennlp.tools.stemmer.PorterStemmer;

public class DocumentFreqWithoutSWR {
	
	public static class DocFreqMapper extends Mapper<Object, Text, Text, Text> {
		// mapper class which takes an Object and Text as input and returns Text and Text as output.

		private Text word = new Text();
		private final static Text result = new Text();

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			PorterStemmer stemmer = new PorterStemmer();

			// getting the file name from the context.
			FileSplit split = (FileSplit) context.getInputSplit();
	        String fileName = split.getPath().getName().toString();

	        // setting the result as file name.
	        this.result.set(fileName);
	        
	        // splitting the words and storing them in an array.
	        String line = value.toString();
			String[] tokens = line.split("[^\\w']+");
			for (String token : tokens) {
				
				token = token.toLowerCase();
				
				this.word.set(stemmer.stem(token));
				context.write(this.word, this.result);
				
			}
		}
	}
	
	public static class DocFreqReducer extends Reducer<Text, Text, Text, IntWritable> {
		// reducer class takes Text, Text as input and returns Text and IntWritable as output.
		
		private IntWritable result = new IntWritable();

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			// finding the DF of the words
			Set<String> s = new HashSet<>();
			for (Text idx : values) {
				s.add(idx.toString());
			}
			this.result.set(s.size());
			context.write(key, this.result);
		}

	}

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "DF");
			
		
		job.setMapperClass(DocFreqMapper.class);
		job.setReducerClass(DocFreqReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1])); 
		
		job.waitForCompletion(true);
		
		String file_name = "part-r-00000";
		String dir_name = args[1];
		String path = dir_name+"/"+file_name;
		
		HashMap<String,Integer> terms = new HashMap<>();
		
		try {
			  FileReader fileReader = new FileReader(path);
			  BufferedReader bufferedReader = new BufferedReader(fileReader);
			  String line;
			  while ((line = bufferedReader.readLine()) != null) {
				  String[] words = line.split("\t");
				  terms.put(words[0], Integer.parseInt(words[1]));
			  }
			  bufferedReader.close();
			  fileReader.close();
			} catch (IOException e) {
			  e.printStackTrace();
		}
		
		// from the output getting the top  100 words with most DF.
		List<Map.Entry<String, Integer>> sortedTerms = new ArrayList<>(terms.entrySet());
        Collections.sort(sortedTerms, (a, b) -> b.getValue().compareTo(a.getValue()));
        
        // Write the top 100 terms to a file
        BufferedWriter writer = new BufferedWriter(new FileWriter(args[1]+"/top_100_terms.txt"));
        for (int i = 0; i < 100 && i < sortedTerms.size(); i++) {
            String term = sortedTerms.get(i).getKey();
            int value = sortedTerms.get(i).getValue();
            writer.write(term + "\t" + value + "\n");
        }
        writer.close();
		
        String file1= "part-r-00000";
		String path1 = args[1];
		String filepath =  path1+"/"+file1;
		
	    
	    File oldFile = new File(filepath);
        File newFile = new File(args[1]+"/DFWSWR.tsv");

        boolean isRenamed = oldFile.renameTo(newFile);

        
		System.exit(0);
	}

}