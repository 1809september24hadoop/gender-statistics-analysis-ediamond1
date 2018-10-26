package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.common.util.concurrent.AtomicDouble;

/**
 * Filters input and writes countries where Female graduation rate is less than 30.0%
 * @author cloudera
 *
 */
public class LowGraduationReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {  	
	
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) 
			throws IOException, InterruptedException{
		AtomicDouble femaleGraduationRate = new AtomicDouble(0.0);
		
		for(DoubleWritable value : values){
			femaleGraduationRate.set(value.get());
		}
		
		if (femaleGraduationRate.get() < 30.0){
			context.write(key, new DoubleWritable(femaleGraduationRate.get()));
		}
		
	}

}
