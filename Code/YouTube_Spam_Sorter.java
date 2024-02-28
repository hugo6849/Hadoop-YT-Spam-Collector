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



public class YouTube_Spam_Sorter{
	
	/**
	 * From the Mapper, we will first get the category of the comment, 1 being spam and 0 being ham it will then
	 * be parsed to the shuffle and sort phase
	 */
	public static class YouTubeMapReducer extends Mapper<LongWritable, Text, Text, Text>{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			 String line = value.toString();
			 String[] words = line.split(","); //gets each of our rows into an array
			 Text id = new Text(words[0]);  //our comment's ID in col. 1 ex. LZQPQhLyRh80UYxNuaDWhIGQYNQ96IuCg
			 Text spamInfo = new Text(words[1] +","+words[4]+","+words[2]+","+words[3]); //our related to the spam (Spam Type, Comment, Author, and Date Posted)
			 context.write(id, spamInfo); //combines our ID, SpamType, and finally, the comment in question
		 }	
	}
	
	public static class YouTubeSpamReducer
	extends Reducer<Text,Text,Text,Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException {
			int commentType = 0; 
			String comment = "";
			String author = "";
			String date = "";
			
			Iterator<Text> valuesIter = values.iterator();
			while(valuesIter.hasNext()){
				String val = valuesIter.next().toString(); // val type, comment
				String [] words = val.split(","); //store these vals to an array
				commentType = Integer.parseInt(words[0]); //we store the type in this line
				
				//if the comment was detected as spam, we will output the comment in question
				if(commentType == 1) {
					comment = words[1];
					date = words[2];
					author = words[3];
					
					context.write(key, new Text("Comment: "+comment+" Author: "+author+" Date: "+date));
				}
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "Spam Output:");
	    job.setJarByClass(YouTube_Spam_Sorter.class);
	    job.setJobName("Type");
	    job.setMapperClass(YouTubeMapReducer.class);
	    job.setReducerClass(YouTubeSpamReducer.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
	
}
