package test.hadoop.session4.assign2.task2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;


public class TelevisionDataMap extends Mapper<LongWritable, Text, Text,IntWritable>{
	
	 public void map(LongWritable key, Text value, Context context)
			 throws IOException, InterruptedException {
	

		 String [] strTokens = value.toString().split("\\|");
		 IntWritable one = new IntWritable(1);
		 
		 if(!strTokens[0].equals("NA") && !strTokens[1].equals("NA"))
		{
			 System.out.println("write");
			context.write(new Text(strTokens[0]), one);
		 }
		 
	 }

}
