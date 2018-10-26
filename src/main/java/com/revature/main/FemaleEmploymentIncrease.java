package com.revature.main;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.map.EmploymentMapperF;
import com.revature.reduce.EmploymentChangeReducerF;

/**
 * Calculates worldwide increase in female employment since 2000
 * @author cloudera
 *
 */
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
		
		job.setMapperClass(EmploymentMapperF.class);		
		job.setReducerClass(EmploymentChangeReducerF.class);
		
		job.setOutputKeyClass(Text.class);			
		job.setOutputValueClass(DoubleWritable.class);
		
		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}

}

