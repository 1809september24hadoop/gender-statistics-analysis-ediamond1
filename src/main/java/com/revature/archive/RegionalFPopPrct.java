package com.revature.archive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RegionalFPopPrct {
	
	public static void main(String[] args) throws Exception {
		
		if (args.length != 3) {
			System.out.println("Usage: RegionalFPopPrct <country dir> <data dir> <output dir>\n");
			System.exit(-1);
		}
		
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Female population percentage by region");
		job.setJarByClass(RegionalFPopPrct.class);
		job.setReducerClass(RegionalAvgReducer.class);
		job.setCombinerClass(FPopJoinReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, RegionMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, FPopPrctMapper.class);
		Path outputPath = new Path(args[2]);
		
		FileOutputFormat.setOutputPath(job, outputPath);
		/*outputPath.getFileSystem(conf).delete(outputPath);*/
		outputPath.getFileSystem(conf);
		System.exit(job.waitForCompletion(true) ? 0 :1);
	}
}
