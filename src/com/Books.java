package com;

import java.io.IOException;
import java.util.*;

import java.util.regex.Matcher;
import java.util.regex.Pattern;        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class Books {

	/**
	 * @param args
	 */
	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
		private static final String titleRe ="(\"[-a-zA-Z]*.txt\")";
		private static final String wordRe ="([\\w]*)";
		private static final Pattern p1 = Pattern.compile(titleRe);
		private static final Pattern p2 = Pattern.compile(wordRe);
		private Text title=new Text();
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			Matcher titlematcher = p1.matcher(value.toString());
			Matcher wordmatcher = p2.matcher(value.toString());
	        //String line = value.toString();
	        //String[] tokens=line.split(",");
	        while(titlematcher.find()){
	        	title.set(titlematcher.group());
	        	break;
	        }
	        //int n=tokens.length;
	        /*for(int i=1;i<n;i++) {
	            String[] words=tokens[i].split(" ");
	            int m=words.length;
	            for(int j=0;j<m;j++){
	            	context.write(new Text(words[j]), title);
	            }
	            
	        }*/
	        boolean skip=wordmatcher.find();//just to skip the first three occurrences to skip the title
	        skip=wordmatcher.find();
	        skip=wordmatcher.find();
	        while(wordmatcher.find()){
	        	String temp=wordmatcher.group();
	        	context.write(new Text(temp), title);
	        }
	    }
	}
	
	public static class Reduce extends Reducer<Text, Text, Text, Text>{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String out="";
			for (Text val : values) {
				out=out.concat(val.toString());
			}
			context.write(key, new Text(out));
		}
	}
	
	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
        
		Job job = new Job(conf, "books - inverted index");
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path("I:\\study\\NITK\\projects\\disributed computing application using java\\beyond the project\\books.json"));
		FileOutputFormat.setOutputPath(job, new Path("I:\\study\\NITK\\projects\\disributed computing application using java\\beyond the project\\booksOutputUbuntu.txt"));
		
		job.waitForCompletion(true);
	}

}
