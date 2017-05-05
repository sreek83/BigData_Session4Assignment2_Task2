package test.hadoop.session4.assign2.task2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;

public class TeleDataDriver {

	public static void main(String[] args) throws IOException,ClassNotFoundException,InterruptedException  {
		
     Configuration conf = new Configuration();
     Job job = new Job(conf,"teledata2");
     
     job.setMapperClass(TeleDataMapper.class);
     
     job.setReducerClass(TeleDataReducer.class);
     
     job.setMapOutputKeyClass(Text.class);
     job.setMapOutputValueClass(IntWritable.class);
     
     job.setInputFormatClass(TextInputFormat.class);
     job.setOutputFormatClass(TextOutputFormat.class);
     
     FileInputFormat.addInputPath(job, new Path(args[0]));
     FileOutputFormat.setOutputPath(job,new Path(args[1]));
     
     job.waitForCompletion(true);
     
	}

}
