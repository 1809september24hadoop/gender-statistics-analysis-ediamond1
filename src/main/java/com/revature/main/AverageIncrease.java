package com.revature.main;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.map.USGraduationMapperF;
import com.revature.reduce.EducationIncreaseFReducer;
import com.revature.reduce.IncreaseAvgReducer;

/**
 * Calculates the average annual increase in Female graduation rate in the US
 * @author cloudera
 *
 */
public class AverageIncrease {
	
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.println("Usage: AverageIncrease <input dir> <output dir>\n");
			System.exit(-1);
		}
		
		Job job = new Job();
		
		job.setJarByClass(AverageIncrease.class);
		
		job.setJobName("Average anual increase in US female education");
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(USGraduationMapperF.class);		
		job.setCombinerClass(EducationIncreaseFReducer.class);
		job.setReducerClass(IncreaseAvgReducer.class);
		
		job.setOutputKeyClass(Text.class);			
		job.setOutputValueClass(DoubleWritable.class);
		
		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}

}
