package com.revature.test;

import java.util.ArrayList;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import com.revature.map.FPopPercentMapper;
import com.revature.reduce.FPopPercentReducer;

public class FPopPercentTest {
	
	private static final Logger LOGGER = Logger.getLogger(FPopPercentTest.class);
	
	private MapDriver<LongWritable, Text, Text, DoubleWritable> mapDriver;
	private ReduceDriver<Text, DoubleWritable, Text, DoubleWritable> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable> mapReduceDriver;
	
	@Before
	public void setup(){
		FPopPercentMapper mapper = new FPopPercentMapper();
		mapDriver = new MapDriver<>();
		mapDriver.setMapper(mapper);
		
		FPopPercentReducer reducer = new FPopPercentReducer();
		reduceDriver = new ReduceDriver<>();
		reduceDriver.setReducer(reducer);
		
		mapReduceDriver = new MapReduceDriver();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
		//		
	}
	
	@Test
	public void testMapper(){
		Text testLine = new Text("\"Albania\",\"ARB\",\"Population, female (% of total)\",\"SP.POP.TOTL.FE.ZS\",\"49.6117841683248\",\"49.608646912577\",\"49.6068459960122\",\"49.6060804228013\",\"49.6058781237938\",\"49.6057000494725\",\"49.6054189012317\",\"49.6045449937528\","
				+ "\"49.6018152648038\",\"49.5957768021229\",\"49.5854280685629\",\"49.5712257190137\",\"49.5534209118605\",\"49.5309086759233\",\"49.5024891215138\",\"49.4679895820584\",\"49.4270051561855\",\"49.3813927887311\",\"49.3356756511479\",\"49.2950993934995\",\"49.2626043119693\",\"49.238918990738\",\"49.2219177261099\",\"49.2081024462324\",\"49.1930405193739\",\"49.1738864116327\","
				+ "\"49.1510836677467\",\"49.1259050146738\",\"49.0978277760222\",\"49.0664005745115\",\"49.0351438496717\",\"48.9961060210256\",\"48.9832147131528\",\"48.9515095322557\",\"48.9316754260399\",\"48.8790463636796\",\"48.8875190872667\",\"48.9092214548234\",\"48.9347156553819\",\"48.9520966561592\",\"48.9532722488563\",\"48.9378563051151\",\"48.9086701083007\",\"48.8655724798038\",\"48.8091586057843\",\"48.7410840715125\","
				+ "\"48.6613754438001\",\"48.5731828915502\",\"48.4847187790163\",\"48.4055706549459\",\"48.3420120717093\",\"48.2952797641094\",\"48.2635223622657\",\"48.2456030589521\",\"48.2394764732753\",\"48.2433242198618\",\"\",");
	
		mapDriver.withInput(new LongWritable(1), testLine);
		
		mapDriver.withOutput(new Text("Albania"), new DoubleWritable(48.2433242198618));
		
		mapDriver.runTest();
		
	}
	
	
	@Test
	public void testReducer(){
		ArrayList<DoubleWritable> values = new ArrayList<>();
		values.add(new DoubleWritable(48.2433242198618));
		
		reduceDriver.withInput(new Text("Albania"), values);
		
		reduceDriver.withOutput(new Text("Albania"), new DoubleWritable(48.2433242198618));
		
		reduceDriver.runTest();
	}
	
	@Test
	public void testMapReducer(){
		Text testLine = new Text("\"Albania\",\"ARB\",\"Population, female (% of total)\",\"SP.POP.TOTL.FE.ZS\",\"49.6117841683248\",\"49.608646912577\",\"49.6068459960122\",\"49.6060804228013\",\"49.6058781237938\",\"49.6057000494725\",\"49.6054189012317\",\"49.6045449937528\","
				+ "\"49.6018152648038\",\"49.5957768021229\",\"49.5854280685629\",\"49.5712257190137\",\"49.5534209118605\",\"49.5309086759233\",\"49.5024891215138\",\"49.4679895820584\",\"49.4270051561855\",\"49.3813927887311\",\"49.3356756511479\",\"49.2950993934995\",\"49.2626043119693\",\"49.238918990738\",\"49.2219177261099\",\"49.2081024462324\",\"49.1930405193739\",\"49.1738864116327\","
				+ "\"49.1510836677467\",\"49.1259050146738\",\"49.0978277760222\",\"49.0664005745115\",\"49.0351438496717\",\"48.9961060210256\",\"48.9832147131528\",\"48.9515095322557\",\"48.9316754260399\",\"48.8790463636796\",\"48.8875190872667\",\"48.9092214548234\",\"48.9347156553819\",\"48.9520966561592\",\"48.9532722488563\",\"48.9378563051151\",\"48.9086701083007\",\"48.8655724798038\",\"48.8091586057843\",\"48.7410840715125\","
				+ "\"48.6613754438001\",\"48.5731828915502\",\"48.4847187790163\",\"48.4055706549459\",\"48.3420120717093\",\"48.2952797641094\",\"48.2635223622657\",\"48.2456030589521\",\"48.2394764732753\",\"48.2433242198618\",\"\",");
		
		mapReduceDriver.withInput(new LongWritable(1), testLine);
		
		mapReduceDriver.withOutput(new Text("Albania"), new DoubleWritable(48.2433242198618));
		
		mapReduceDriver.runTest();

	}

}
