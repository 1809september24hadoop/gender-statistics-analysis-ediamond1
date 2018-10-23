package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class USEmploymentMapperF extends Mapper<LongWritable, Text, Text, DoubleWritable>{		//generics are input K/V, output K/V, will depend on the actual task

	@Override
	public void map(LongWritable key, Text value, Context context)  				//context abstracts where we're writing
			throws IOException, InterruptedException{

		String line = value.toString();
		String[] parsedLines = line.substring(1, line.length()-1).split("\",\"");

		if (parsedLines[1].equals("USA") && parsedLines[3].equals("SL.EMP.TOTL.SP.FE.NE.ZS")){		
			String country = parsedLines[0];
//			for(int i = 0 ; i < parsedLines.length; i++){
//				System.out.println("string is [" + parsedLines[i] + "]");
//			}

			for(int i = 44; i < parsedLines.length; i++){							//this is the range of columns where actual numeric data is found (starting at the year 2000
				String cellData = parsedLines[i];
				try{
					if (cellData.length() > 0){						//write to map output for every non-empty cell, to get every datapoint.
						context.write(new Text(country), new DoubleWritable(Double.valueOf(cellData)));
					}
				}catch(NumberFormatException e){

				}
			}
		}

	}

}

