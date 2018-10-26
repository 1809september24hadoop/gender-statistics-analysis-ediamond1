package com.revature.main;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.map.GraduationMapper;
import com.revature.reduce.LowGraduationReducer;

/**
 * Finds countries where female graduation rate is less that 30%
 * @author cloudera
 *
 */
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
		
		job.setMapperClass(GraduationMapper.class);		
		job.setReducerClass(LowGraduationReducer.class);
		
		job.setOutputKeyClass(Text.class);			
		job.setOutputValueClass(DoubleWritable.class);
		
		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}

}
