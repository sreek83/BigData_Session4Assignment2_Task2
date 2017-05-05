package test.hadoop.session4.assign2.task2;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;


public class TeleDataMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	public void map(LongWritable key,Text values,Context context) throws IOException,InterruptedException {
		
		String [] strs = values.toString().split("\\|");
		if(strs[0].equals("Onida")) {
			
			context.write(new Text(strs[3]), new IntWritable(1));
		}
		
	}

}
