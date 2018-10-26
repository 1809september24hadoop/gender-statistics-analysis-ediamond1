package com.revature.archive;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RegionMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		
		String line = value.toString();
		String[] parsedLines = line.substring(1,line.length()-1).split("\",\"");
		
		context.write(new Text(parsedLines[0]), new Text("region\t" + parsedLines[7]));
	}
}
