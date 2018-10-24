package com.revature.map;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class RegionalFPopPrctMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	
	private static Logger LOGGER = Logger.getLogger(RegionalFPopPrctMapper.class);
	
	private Map<String, String> regionMap = new HashMap<String, String>();
	
	protected void setup(Context context) throws java.io.IOException,
	InterruptedException {
		LOGGER.info("Setup");
		Path[] files = DistributedCache.getLocalCacheFiles(context
				.getConfiguration());
		for (Path p : files) {
			if (p.getName().equals("Gender_StatsCountry.csv")) {
				BufferedReader reader = new BufferedReader(new FileReader(
						p.toString()));
				String line = reader.readLine();
				while (line != null) {
					LOGGER.info(line);
					String[] tokens = line.substring(1, line.length()-1).split("\",\"");
					System.out.println(tokens.toString());
					String countryCode = tokens[0];
					String region = tokens[7];
					regionMap.put(countryCode, region);
					line = reader.readLine();
				}
			}
		}
		if (regionMap.isEmpty()) {
			throw new IOException("Unable to load Abbrevation data.");
		}
	}
	
	@Override
	public void map(LongWritable key, Text value, Context context)  				//context abstracts where we're writing
			throws IOException, InterruptedException{

		String line = value.toString();
		String[] parsedLines = line.substring(1, line.length()-1).split("\",\"");

		if (parsedLines[3].equals("SP.POP.TOTL.FE.ZS")){		
			String countryCode = parsedLines[1];
			String region = regionMap.get(countryCode);
			double current = 0;

			for(int i = 4; i < parsedLines.length; i++){							//this is the range of columns where actual numeric data is found (starting at the year 2000
				String cellData = parsedLines[i];
				try{
					if (cellData.length() > 0){						//write to map output for every non-empty cell, to get every datapoint.
						current = Double.valueOf(cellData);
					}
				}catch(NumberFormatException e){

				}
			}
			context.write(new Text(region), new DoubleWritable(current));
		}
	}
}
