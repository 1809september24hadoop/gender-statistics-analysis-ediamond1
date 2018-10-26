package com.revature.main;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.map.FPopPercentMapper;
import com.revature.reduce.FPopPercentReducer;
import com.revature.util.FPopPartitioner;

/**
 * Separates countries by whether or not the majority of the population is female
 * @author cloudera
 *
 */
public class FPopPercent {
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.println("Usage: FPopPercent <input dir> <output dir>\n");
			System.exit(-1);
		}
		
		Job job = new Job();
		
		job.setJarByClass(FPopPercent.class);
		
		job.setJobName("Grouping Countries by whether or not Female population is majority");
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(FPopPercentMapper.class);		
		
		job.setPartitionerClass(FPopPartitioner.class);
		
		job.setNumReduceTasks(2);
		
		job.setReducerClass(FPopPercentReducer.class);
		
		job.setOutputKeyClass(Text.class);			
		job.setOutputValueClass(DoubleWritable.class);
		
		job.setMapOutputKeyClass(Text.class);			
		job.setMapOutputValueClass(DoubleWritable.class);
		
		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}

}
