package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

/**
 * Writes partitioned data; redundant unless in conjunction with FPopPartitioner
 * @author cloudera
 *
 */
public class FPopPercentReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) 
			throws IOException, InterruptedException{
		DoubleWritable percentage = new DoubleWritable();
		for (DoubleWritable val : values){
			percentage = val;
		}
		if (percentage.get()>0.0){
			context.write(key, percentage);
		}
		
	}

}
