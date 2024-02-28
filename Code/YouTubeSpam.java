package org.YouTube_Spam;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.YouTube_Spam.WordCountDriver.IntSumReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class YouTube_Spam {
	
	/**
	 * From the Mapper, we will first get the category of the comment, 1 being spam and 0 being ham it will then
	 * be parsed to the shuffle and sort phase
	 */
	public static class YouTubeMapReducer extends Mapper<LongWritable, Text, Text, IntWritable>{
		private final static IntWritable one = new IntWritable(1);
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			 String line = value.toString();
			 String SpamData = line.split(",")[1];
			 context.write(new Text(SpamData), one);
		 }	
	}
	
	public static class YouTubeSpamReducer
	extends Reducer<Text,IntWritable,Text,IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context) 
				throws IOException, InterruptedException {
			int frequency = 0;
			for (IntWritable value : values) {
				frequency += value.get();	
			}
			context.write(key, new IntWritable(frequency));
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "spam");
	    job.setJarByClass(YouTube_Spam.class);
	    job.setJobName("Frequency");
	    job.setMapperClass(YouTubeMapReducer.class);
	    job.setCombinerClass(YouTubeSpamReducer.class);
	    job.setReducerClass(YouTubeSpamReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
	
}