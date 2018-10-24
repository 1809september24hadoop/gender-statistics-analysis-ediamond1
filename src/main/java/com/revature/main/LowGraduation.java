package com.revature.main;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.map.GraduationMapper;
import com.revature.reduce.LowGraduationReducer;

public class LowGraduation {
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.println("Usage: LowGraduation <input dir> <output dir>\n");
			System.exit(-1);
		}
		
		Job job = new Job();
		
		job.setJarByClass(LowGraduation.class);
		
		job.setJobName("Countries with < 30% rate of female graduation");
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(GraduationMapper.class);		//these validate the mapper and reducer extend Mapper and Reducer
		
		//job.setNumReduceTasks(0);
		
		//Output of combiner will be input of actual reducer
		job.setReducerClass(LowGraduationReducer.class);
		
		job.setOutputKeyClass(Text.class);			//These don't have validation
		job.setOutputValueClass(DoubleWritable.class);
		
//		job.setMapOutputKeyClass(Text.class);			//These don't have validation
//		job.setMapOutputValueClass(DoubleWritable.class);
		
		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}

}
