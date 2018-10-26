package com.revature.reduce;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.common.util.concurrent.AtomicDouble;

/**
 * Averages the annual change in graduation rate, give an input of those changes
 * @author cloudera
 *
 */
public class IncreaseAvgReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {  	//our input is the output of the Mapper
	
	
	AtomicInteger size = new AtomicInteger(0);
	AtomicDouble sum = new AtomicDouble(0.0);
	
	public void reduce(Text key, Iterable<DoubleWritable> increases, Context context) 
			throws IOException, InterruptedException{
		for (DoubleWritable increase : increases){
			sum = new AtomicDouble(sum.get()+increase.get());
			size=new AtomicInteger(size.get()+1);
		}
		DoubleWritable averageIncrease = new DoubleWritable(Math.abs(sum.get()/size.get()));
		context.write(new Text("Average yearly increase in US Female Graduation since 2000:"), averageIncrease);
	}

}