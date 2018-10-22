package com.revature.map;

import java.io.IOException;
import java.io.StringReader;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.opencsv.CSVReader;


public class GraduationMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{		//generics are input K/V, output K/V, will depend on the actual task

	@Override
	public void map(LongWritable key, Text value, Context context)  				//context abstracts where we're writing
			throws IOException, InterruptedException{

		String line = value.toString();
		String[] parsedLines = line.substring(1, line.length()-1).split("\",\"");

		//CSVReader reader = new CSVReader(new StringReader(line));

		//String[] parsedLines = reader.readNext();
		//reader.close();

		if (parsedLines[2].equals("Gross graduation ratio, tertiary, female (%)")){		
			String country = parsedLines[0];
//			for(int i = 0 ; i < parsedLines.length; i++){
//				System.out.println("string is [" + parsedLines[i] + "]");
//			}

			for(int i = 4; i < parsedLines.length; i++){							//this is the range of columns where actual numeric data is found
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
