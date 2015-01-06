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

public class Friends {

	/**
	 * @param args
	 */
	//a fan and all the celebrities he follows[fan to celebrities]
	public static class MapFtoC extends Mapper<LongWritable,Text,Text,Text>{
		private static final String name ="([\\w]*)";
		private static final Pattern p = Pattern.compile(name);
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			Matcher namematcher = p.matcher(value.toString());
			namematcher.find();
			String f=namematcher.group();
			namematcher.find();
			String c=namematcher.group();
			context.write(new Text(f), new Text(c));
		}
	}
	
	public static class ReduceFtoC extends Reducer<Text,Text,Text,Text>{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			String delim="\"";
			String out="";
			for (Text val : values) {
				out=out.concat(delim);
				out=out.concat(val.toString());
				out=out.concat(delim);
			}
			context.write(key, new Text(out));
		}
	}
	//a celebrity followed by all his fans[celebrity to fans]
	public static class MapCtoF extends Mapper<LongWritable,Text,Text,Text>{
		private static final String name ="([\\w]*)";
		private static final Pattern p = Pattern.compile(name);
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			Matcher namematcher = p.matcher(value.toString());
			namematcher.find();
			String f=namematcher.group();
			namematcher.find();
			String c=namematcher.group();
			context.write(new Text(c), new Text(f));
		}
	}
	
	public static class ReduceCtoF extends Reducer<Text,Text,Text,Text>{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			String delim="\"";
			String out="";
			for (Text val : values) {
				out=out.concat(delim);
				out=out.concat(val.toString());
				out=out.concat(delim);
			}
			context.write(key, new Text(out));
		}		
	}
	
	public static class MapFinal extends Mapper<LongWritable,Text,Text,Text>{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			
		}
	}
	
	public static class ReduceFinal extends Reducer<Text,Text,Text,Text>{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			
		}		
	}
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		//job configuration for FtoC
		Configuration conf1 = new Configuration();
        
		Job job1 = new Job(conf1, "friendsFtoC - inverted index");
		
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		
		job1.setMapperClass(MapFtoC.class);
		job1.setReducerClass(ReduceFtoC.class);
		
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job1, new Path("I:\\study\\NITK\\projects\\disributed computing application using java\\beyond the project\\Friends\\friends.json"));
		FileOutputFormat.setOutputPath(job1, new Path("I:\\study\\NITK\\projects\\disributed computing application using java\\beyond the project\\Friends\\FriendsOutput\\FtoCout1"));
		
		job1.waitForCompletion(true);
		
		//job configuration for CtoF
		Configuration conf2 = new Configuration();
        
		Job job2 = new Job(conf2, "friendsCtoF - inverted index");
		
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		
		job2.setMapperClass(MapCtoF.class);
		job2.setReducerClass(ReduceCtoF.class);
		
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job2, new Path("I:\\study\\NITK\\projects\\disributed computing application using java\\beyond the project\\Friends\\friends.json"));
		FileOutputFormat.setOutputPath(job2, new Path("I:\\study\\NITK\\projects\\disributed computing application using java\\beyond the project\\FriendsOutput\\CtoFout1"));
		
		job2.waitForCompletion(true);
		
		//job configuration for Final Map Reduce
		Configuration conf3 = new Configuration();
        
		Job job3 = new Job(conf3, "friendsFinal(Mutual Friends) - inverted index");
		
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
		
		job3.setMapperClass(MapFtoC.class);
		job3.setReducerClass(ReduceFtoC.class);
		
		job3.setInputFormatClass(TextInputFormat.class);
		job3.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job3, new Path("I:\\study\\NITK\\projects\\disributed computing application using java\\beyond the project\\"));
		FileOutputFormat.setOutputPath(job3, new Path("I:\\study\\NITK\\projects\\disributed computing application using java\\beyond the project\\"));
		
		job3.waitForCompletion(true);		
	}

}
