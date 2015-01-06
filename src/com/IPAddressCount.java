package com;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class IPAddressCount {

	/**
	 * @param args
	 */
	//Map class
	public static class Map extends Mapper<LongWritable,Text,Text,IntWritable>{
		private static final IntWritable one = new IntWritable(1);
		private Text IPAddress = new Text();
		private static final String IP = "[\\[](0?[1-9]{1,2}|1[\\d][\\d]|2[0-4][\\d]|2[5][0-5])[.](0?[\\d]{1,2}|1[\\d][\\d]|2[0-4][\\d]|2[5][0-5])[.](0?[\\d]{1,2}|1[\\d][\\d]|2[0-4][\\d]|2[5][0-5])[.](0?[\\d]{1,2}|1[\\d][\\d]|2[0-4][\\d]|2[5][0-5])[\\]]";
		private static final Pattern p = Pattern.compile(IP);
	    
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			Matcher IPmatcher = p.matcher(value.toString());
			while(IPmatcher.find()){
				IPAddress.set(IPmatcher.group(0));
				context.write(IPAddress,one);
			}
		}
	}
	//Reduce class
	public static class Reduce extends Reducer<Text,Iterable<IntWritable>,Text,Text>{
		public void reduce(Text key,Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
			int sum = 0;
	        for (IntWritable val : values) {
	            sum += val.get();
	        }
	        String t = key.toString();
	        context.write(new Text(String.format("\n%s", t)), new Text(String.format("%d\n", sum)));
		}
	}
	//Main function
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
        
        Job job = new Job(conf, "IPAddressCount");
    
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        FileInputFormat.addInputPath(job, new Path("C:\\Users\\Bhuvan\\workspace\\Hadoop-Test-1\\t.txt"));
        FileOutputFormat.setOutputPath(job, new Path("C:\\Users\\Bhuvan\\workspace\\Hadoop-Test-1\\t1out"));
        
        job.waitForCompletion(true);
	}

}

