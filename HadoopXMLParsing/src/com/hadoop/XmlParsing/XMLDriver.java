package com.hadoop.XmlParsing;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class XMLDriver {

	public static void main(String[] args) {
		try{
		Configuration conf = new Configuration();
		String[] arg = new GenericOptionsParser(conf, args).getRemainingArgs();
		conf.set("START_TAG_KEY", "<property>");
		conf.set("END_TAG_KEY", "</property>");
		Job job = new Job(conf, "XML Processing Processing");
		job.setJarByClass(XMLDriver.class);
		job.setMapperClass(MyMapper.class);
		job.setNumReduceTasks(0);
		job.setInputFormatClass(XmlInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
		} catch(Exception e) {
		System.out.println(e.getMessage().toString());
		}
	}
}
