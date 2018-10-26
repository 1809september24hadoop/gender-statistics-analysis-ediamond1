package com.revature.archive;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import com.google.common.util.concurrent.AtomicDouble;

public class RegionalAvgReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	
	AtomicInteger size = new AtomicInteger(0);
	AtomicDouble sum = new AtomicDouble(0.0);
	
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) 
			throws IOException, InterruptedException{
		for (DoubleWritable value : values){
			sum = new AtomicDouble(sum.get()+value.get());
			size=new AtomicInteger(size.get()+1);
		}
		DoubleWritable average = new DoubleWritable(sum.get()/size.get());
		context.write(key, average);
	}

}
