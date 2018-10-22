package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LowGraduationReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {  	//our input is the output of the Mapper
	
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) 
			throws IOException, InterruptedException{
		double femaleGraduationRate = 0.0;
		
		for(DoubleWritable value: values){
			femaleGraduationRate = value.get();
		}
		
		if (femaleGraduationRate < 30.0){
			context.write(key, new DoubleWritable(femaleGraduationRate));
		}
		
	}

}
