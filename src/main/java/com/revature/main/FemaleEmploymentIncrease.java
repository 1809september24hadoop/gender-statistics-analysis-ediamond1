package com.revature.main;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.map.EmploymentMapperF;
import com.revature.reduce.EmploymentChangeReducerF;

public class FemaleEmploymentIncrease {
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.println("Usage: FemaleEmploymentIncrease <input dir> <output dir>\n");
			System.exit(-1);
		}
		
		Job job = new Job();
		
		job.setJarByClass(FemaleEmploymentIncrease.class);
		
		job.setJobName("Increase in female employment since 2000");
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(EmploymentMapperF.class);		//these validate the mapper and reducer extend Mapper and Reducer
		
		//Output of combiner will be input of actual reducer
		job.setReducerClass(EmploymentChangeReducerF.class);
		
//		job.setNumReduceTasks(0);
		
		job.setOutputKeyClass(Text.class);			//These don't have validation
		job.setOutputValueClass(DoubleWritable.class);
		
//		job.setMapOutputKeyClass(Text.class);
//		job.setMapOutputValueClass(DoubleWritable.class);
		
		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}

}

