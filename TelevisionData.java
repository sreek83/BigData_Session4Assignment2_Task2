package test.hadoop.session4.assign2.task2;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.IOException;


public class TelevisionData {

	public static void main(String[] args) throws IOException,ClassNotFoundException,InterruptedException {
		
		Configuration conf = new Configuration();
		Job job = new Job(conf,"teleData");
		
		job.setMapperClass(TelevisionDataMap.class);
		job.setReducerClass(TelevisionDataReduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		
		FileInputFormat.addInputPath(job, new Path (args[0]));
		FileOutputFormat.setOutputPath(job,new Path( args[1]));
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		
		System.exit(job.waitForCompletion(true) ? 0:1); 
		
		

	}

}
