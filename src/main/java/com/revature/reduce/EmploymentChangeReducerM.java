package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.common.util.concurrent.AtomicDouble;

/**
 * Calculates the change in Employment rates over the period of data recieved
 * @author cloudera
 *
 */
public class EmploymentChangeReducerM extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {  	//our input is the output of the Mapper
	
	static AtomicDouble first = new AtomicDouble(0.0);
	static AtomicDouble increase = new AtomicDouble(0.0);
	static AtomicDouble ratio = new AtomicDouble(0.0);
	
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) 
			throws IOException, InterruptedException{
		
		boolean firstFlag = true;
		for (DoubleWritable value : values){
			if(firstFlag){
				firstFlag = false;
				first = new AtomicDouble(value.get());
			}else{
				increase = new AtomicDouble(value.get()-first.get());
			}
		}
		ratio = new AtomicDouble(-1*increase.get()/first.get());
		
		context.write(new Text("Increase in employed percentage of male poputaion since 2000:"), new DoubleWritable(-1*increase.get()));
		context.write(new Text("Increase in male employment since 2000 as a ratio:"), new DoubleWritable(ratio.get()));
		
	}

}

