package com.revature.main;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.map.EmploymentMapperM;
import com.revature.reduce.EmploymentChangeReducerM;

public class MaleEmploymentIncrease {
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.println("Usage: MaleEmploymentIncrease <input dir> <output dir>\n");
			System.exit(-1);
		}
		
		Job job = new Job();
		
		job.setJarByClass(MaleEmploymentIncrease.class);
		
		job.setJobName("Increase in US male employment since 2000");
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(EmploymentMapperM.class);		//these validate the mapper and reducer extend Mapper and Reducer
		
		//Output of combiner will be input of actual reducer
		job.setReducerClass(EmploymentChangeReducerM.class);
		
//		job.setNumReduceTasks(0);
		
		job.setOutputKeyClass(Text.class);			//These don't have validation
		job.setOutputValueClass(DoubleWritable.class);
		
//		job.setMapOutputKeyClass(Text.class);
//		job.setMapOutputValueClass(DoubleWritable.class);
		
		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}

}

