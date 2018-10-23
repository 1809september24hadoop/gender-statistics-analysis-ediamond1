package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.common.util.concurrent.AtomicDouble;
import com.revature.util.Rounder;

public class EducationIncreaseFReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {  	//our input is the output of the Mapper
	
	static AtomicDouble old = new AtomicDouble(0.0);
	
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) 
			throws IOException, InterruptedException{

		Boolean firstFlag = true;
		
		for (DoubleWritable value : values){
			if (firstFlag){
				firstFlag=false;
			} else {
				double increase;
				if (value.get() >= old.get()){
					increase = Math.abs(value.get() - old.get());
				} else {
					increase = -1*Math.abs(value.get() - old.get());
				}
				context.write(new Text("increase"), new DoubleWritable(Rounder.round(increase, 3)));	
			}
			old = new AtomicDouble(value.get());
		}
		
	}

}