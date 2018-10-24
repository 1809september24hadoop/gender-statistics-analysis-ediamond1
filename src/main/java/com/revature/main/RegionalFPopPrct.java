package com.revature.main;

import java.net.URI;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import com.revature.map.RegionalFPopPrctMapper;
import com.revature.reduce.RegionalFPopPrctReducer;

public class RegionalFPopPrct {
	
	public static void main(String[] args) throws Exception {
		
		if (args.length != 2) {
			System.out.println("Usage: MaleEmploymentIncrease <input dir> <output dir>\n");
			System.exit(-1);
		}
		
		Job job = new Job();
		
		job.setJarByClass(RegionalFPopPrct.class);
		
		job.setJobName("Increase in US male employment since 2000");
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(RegionalFPopPrctMapper.class);		//these validate the mapper and reducer extend Mapper and Reducer
		
		//Output of combiner will be input of actual reducer
		job.setReducerClass(RegionalFPopPrctReducer.class);
		
//		job.setNumReduceTasks(0);
		
		job.setOutputKeyClass(Text.class);			//These don't have validation
		job.setOutputValueClass(DoubleWritable.class);
		
		try {
			DistributedCache.addCacheFile(new URI("hdfs://quickstart.cloudera:8020/user/cloudera/HData/Gender-stats/Gender_StatsCountry.csv"),
					job.getConfiguration());
		} catch (Exception e) {
			System.out.println(e);
		}
		
//		job.setMapOutputKeyClass(Text.class);
//		job.setMapOutputValueClass(DoubleWritable.class);
		
		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}

}
