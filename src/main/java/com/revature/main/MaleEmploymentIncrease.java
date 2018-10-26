package com.revature.main;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.map.EmploymentMapperM;
import com.revature.reduce.EmploymentChangeReducerM;

/**
 * Calculates worldwide increase in male employment since 2000
 * @author cloudera
 *
 */
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
		
		job.setMapperClass(EmploymentMapperM.class);		
		job.setReducerClass(EmploymentChangeReducerM.class);
		
		job.setOutputKeyClass(Text.class);			
		job.setOutputValueClass(DoubleWritable.class);
		
		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}

}

