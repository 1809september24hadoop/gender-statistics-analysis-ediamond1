package com.revature.reduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.common.util.concurrent.AtomicDouble;

public class LowGraduationReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {  	//our input is the output of the Mapper
	
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) 
			throws IOException, InterruptedException{
		AtomicDouble femaleGraduationRate = new AtomicDouble(0.0);
		
		List<DoubleWritable> vals = new ArrayList<DoubleWritable>();
		
		for(DoubleWritable value: values){
			femaleGraduationRate.set(value.get());
		}
		
		if (femaleGraduationRate.get() < 30.0){
			context.write(key, new DoubleWritable(femaleGraduationRate.get()));
		}
		
	}

}
