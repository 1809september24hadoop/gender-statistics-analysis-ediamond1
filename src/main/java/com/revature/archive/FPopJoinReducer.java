package com.revature.archive;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FPopJoinReducer extends Reducer<Text, Text, Text, DoubleWritable>  {
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		String region = "";
		double fPopPrct = 0.0;
		for (Text t : values){
			String parts[] = t.toString().split("\t");
			if(parts[0].equals("region")){
				region = parts[1];
			} else if (parts[0].equals("FPopPrct")){
				fPopPrct = Double.parseDouble(parts[1]);
			}
		}
		context.write(new Text(region), new DoubleWritable(fPopPrct));
	}

}
