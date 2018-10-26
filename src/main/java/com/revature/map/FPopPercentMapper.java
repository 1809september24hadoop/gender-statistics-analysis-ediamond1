package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Writes most recent data point for each country, female population as percentage of total
 */
public class FPopPercentMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

		String line = value.toString();
		String[] parsedLines = line.substring(1,line.length()-1).split("\",\"");

		double fPopPrct = 0;

		if (parsedLines[3].equals("SP.POP.TOTL.FE.ZS")){

			for (int i = parsedLines.length -1; i>0; i--){
				try{
					fPopPrct = Double.parseDouble(parsedLines[i]);
					break;
				}catch(NumberFormatException e){
					continue;
				}catch(NullPointerException e){
					continue;
				}
			}
			context.write(new Text(parsedLines[0]), new DoubleWritable(fPopPrct));
		}
	}
}
