package test.hadoop.session4.assign2.task2;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;

public class TeleDataReducer extends Reducer<Text,IntWritable ,Text, IntWritable> {
	
	public void reduce(Text key,Iterable<IntWritable> values,Context context)throws IOException,InterruptedException{
		
		int sum = 0;
		for(IntWritable value : values){
			System.out.println("text "+key.toString());
			sum += value.get();
		}
		context.write(key, new IntWritable(sum));
	}
	
}
