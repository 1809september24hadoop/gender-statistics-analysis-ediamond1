package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Used to write US data for Gross graduation ratio, tertiary, female (%), since the year 2000
 */
public class USGraduationMapperF extends Mapper<LongWritable, Text, Text, DoubleWritable>{	

	@Override
	public void map(LongWritable key, Text value, Context context)  				
			throws IOException, InterruptedException{

		String line = value.toString();
		String[] parsedLines = line.substring(1, line.length()-1).split("\",\"");

		if (parsedLines[1].equals("USA") && parsedLines[3].equals("SE.TER.CMPL.FE.ZS")){		
			String country = parsedLines[0];

			for(int i = 44; i < parsedLines.length; i++){							
				String cellData = parsedLines[i];
				try{
					if (cellData.length() > 0){					
						context.write(new Text(country), new DoubleWritable(Double.valueOf(cellData)));
					}
				}catch(NumberFormatException e){

				}
			}
		}

	}

}
