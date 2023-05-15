package bm25;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;

import opennlp.tools.stemmer.PorterStemmer;

public class TermFrequency {
	
	public static double b = 0.75;
	private static Set<String> files_list = new HashSet<>();
	private static HashMap<String, Integer> dfMap = new HashMap<String, Integer>();
	private static HashMap<String, Integer> dlMap = new HashMap<String, Integer>();

	
	
	public static class TermFreqMapper extends Mapper<Object, Text, Text, MapWritable> {
		// Mapper class with Object, Text as input and Text, MapWritable as output.
		
		private final static IntWritable one = new IntWritable(1);
		private MapWritable myMap = new MapWritable();
		
		@Override
	    public void setup(Context context) throws IOException, InterruptedException {
			
			// getting the cache file from the context 
	        URI[] cacheFiles = context.getCacheFiles();
	        if (cacheFiles != null && cacheFiles.length > 0) {
	            try {
	            	// reading it and storing it in a hashmap.
	                BufferedReader bufferedReader = new BufferedReader(new FileReader(cacheFiles[0].toString()));
	                String line;
	                while ((line = bufferedReader.readLine()) != null) {
	                    String[] words = line.trim().split("\t");
                        if(words.length > 1) dfMap.put(words[0], Integer.parseInt(words[1]));
	                }
	                bufferedReader.close();
	            } catch (Exception e) {
	            	System.out.println("Unable to read the File");
	            	e.printStackTrace();
	                System.exit(1);
	            }
	            
	            try {
	            	// reading it and storing it in a hashmap.
	                BufferedReader bufferedReader = new BufferedReader(new FileReader(cacheFiles[1].toString()));
	                String line;
	                while ((line = bufferedReader.readLine()) != null) {
	                    String[] words = line.trim().split("\t");
                        if(words.length > 1)dlMap.put(words[0], Integer.parseInt(words[1]));
	                }
	                bufferedReader.close();
	            } catch (Exception e) {
	            	System.out.println("Unable to read the File");
	            	e.printStackTrace();
	                System.exit(1);
	            }
	        }
	    }
		
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			Text word = new Text();
			PorterStemmer stemmer = new PorterStemmer();
			MapWritable myMap = new MapWritable();		
			
			FileSplit split = (FileSplit) context.getInputSplit();
	        String fileName = split.getPath().getName().toString();
	        
	        files_list.add(fileName);
	        String line = value.toString();
			String[] tokens = line.split("[^\\w']+");
			
			Set<String> keysSet = dfMap.keySet();
			
			for (String token : tokens) {
				
				token = token.toLowerCase();
				token = stemmer.stem(token);
				
				if(keysSet.contains(token))
				{					
					 if(myMap.containsKey(new Text(token))){
			                IntWritable temp = (IntWritable)myMap.get(new Text(token));
			                int x = temp.get();
			                x++;
			                temp.set(x);
			                myMap.put(new Text(token), temp);
			         } 
					 else {
			                myMap.put(new Text(token), new IntWritable(1));
			        } 
					
				}
			}
			
			word.set(fileName);
            context.write(word, myMap);
		}
	}
	
	public static class TermFreqReducer extends Reducer<Text, MapWritable, Text, DoubleWritable> {
		
		@Override
		public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
			
			MapWritable ret_tf = new MapWritable();
//			MapWritable ret_idf = new MapWritable();

			DoubleWritable result = new DoubleWritable();
			Text output_key = new Text();
			
			// all files with the same file name will be assigned to the same reducer.
			String fileName = key.toString();
			
			// combining all the maps based on the file name and the words.
			
			// calculating TF
            for (MapWritable value : values) {
                for(MapWritable.Entry<Writable, Writable> e : value.entrySet()){
                    if (ret_tf.containsKey(e.getKey())) {
                        int i = ((IntWritable)e.getValue()).get();
                        int j = ((IntWritable)ret_tf.get(e.getKey())).get();
                        ret_tf.put(e.getKey(), new IntWritable(i+j));
                    } else {
                        ret_tf.put(e.getKey(), e.getValue());
                    }
                }
            }
            
//            
//            // computing and writing the TFIDF score into the context.
//            // (word, val) for filename
//            for(MapWritable.Entry<Writable, Writable> e : ret.entrySet()) {
//            	int tf = ((IntWritable)ret.get(e.getKey())).get(); // gets value of the word.
//            	int df = dfMap.get(e.getKey().toString());
//            	double score = tf*Math.log(1+(files_list.size() * 1.0)/df);
//            	result.set(score);
//            	output_key.set(fileName + "\t" + e.getKey().toString());
//            	context.write(output_key, result);
//            }
            
            
          // computing and writing the IDF VALUES AND BM25 score into the context.
          // (word, val) for filename
         int dl = dlMap.get(fileName);
         double L = 0;
         for(String s: dlMap.keySet()) {
        	 L+=dlMap.get(s);
         }
         L=L/files_list.size();
         
         
         for(MapWritable.Entry<Writable, Writable> e : ret_tf.entrySet()) {
          	int tf = ((IntWritable)ret_tf.get(e.getKey())).get(); // gets value of the word.
          	int df = dfMap.get(e.getKey().toString());
          	
          	double idf = Math.log((files_list.size()-df+ 0.5)/(df+ 0.5));
          	double bm25 = idf* ((tf*3)/(tf+ 2*(1-b + b*(dl/L))));
          	
          	result.set(bm25);
          	output_key.set(fileName + "\t" + e.getKey().toString());
          	context.write(output_key, result);
          }
          
            
		}
	}
	
	

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "TF");
		
		String file= "top_100_terms.txt";
		String path = args[1];
		String cache_file_path =  path+"/"+file;
		
		String file2 = "DL.tsv";
		String path2 = args[2];
		String cache_file_path2 = path2+"/"+file2;
		
		if(args.length==5) {
			b = Double.parseDouble(args[4]);
		}
		
		// add the file to the DistributedCache 
		job.addCacheFile(new URI(cache_file_path)); 
		job.addCacheFile(new URI(cache_file_path2)); 
		
		job.setMapperClass(TermFreqMapper.class);
		job.setReducerClass(TermFreqReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MapWritable.class);
			

		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		FileOutputFormat.setOutputPath(job, new Path(args[3]));

		job.waitForCompletion(true);
	    
		String file1= "part-r-00000";
		String path1 = args[3];
		String filepath =  path1+"/"+file1;
		
		
	    
	    File oldFile = new File(filepath);
        File newFile = new File(args[3]+"/BM25.tsv");

        boolean isRenamed = oldFile.renameTo(newFile);
	    
	    System.exit(0);
	}

}