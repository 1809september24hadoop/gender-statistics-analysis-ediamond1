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

import com.revature.main.RegionalFPopPrct;
import com.revature.map.GraduationMapper;
import com.revature.reduce.LowGraduationReducer;

public class GraduationTest {
	
	// test Map, test Reduce, test Map-Reduce combo
	
	private static Logger LOGGER = Logger.getLogger(GraduationTest.class);
	
	private MapDriver<LongWritable, Text, Text, DoubleWritable> mapDriver;
	private ReduceDriver<Text, DoubleWritable, Text, DoubleWritable> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable> mapReduceDriver;
	
	@Before
	public void setup(){
		GraduationMapper mapper = new GraduationMapper();
		mapDriver = new MapDriver<>();
		mapDriver.setMapper(mapper);
		
		LowGraduationReducer reducer = new LowGraduationReducer();
		reduceDriver = new ReduceDriver<>();
		reduceDriver.setReducer(reducer);
//		
		mapReduceDriver = new MapReduceDriver();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}
	
	@Test
	public void testMapper(){
		Text test = new Text("\"TestCountry\",\"\",\"Gross graduation ratio, tertiary, female (%)\",\"\",\"\",\"20.6\",\"25.8\",\"27.6\",\"30.7\",\"29.9\",");
		mapDriver.withInput(new LongWritable(1), test);
		
		//System.out.println(test.toString());
		
		mapDriver.withOutput(new Text("TestCountry"), new DoubleWritable(20.6));
		mapDriver.withOutput(new Text("TestCountry"), new DoubleWritable(25.8));
		mapDriver.withOutput(new Text("TestCountry"), new DoubleWritable(27.6));
		mapDriver.withOutput(new Text("TestCountry"), new DoubleWritable(30.7));
		mapDriver.withOutput(new Text("TestCountry"), new DoubleWritable(29.9));
		
		mapDriver.runTest();
	}
	
	@Test
	public void testReducer(){
		ArrayList<DoubleWritable> values = new ArrayList<DoubleWritable>();
		values.add(new DoubleWritable(20.6));
		values.add(new DoubleWritable(25.8));
		values.add(new DoubleWritable(27.6));
		values.add(new DoubleWritable(30.7));
		values.add(new DoubleWritable(29.9));
		
		reduceDriver.withInput(new Text("TestCountry"), values);
		
		reduceDriver.withOutput(new Text("TestCountry"), new DoubleWritable(29.9));
		
		reduceDriver.runTest();
	}
	
	@Test
	public void testMapReducer(){
		mapReduceDriver.withInput(new LongWritable(1), new Text("\"TestCountry\",\"\",\"Gross graduation ratio, tertiary, female (%)\",\"\",\"\",\"20.6\",\"25.8\",\"27.6\",\"30.7\",\"29.9\","));	//input of mapper
		
		mapReduceDriver.addOutput(new Text("TestCountry"), new DoubleWritable(29.9));
		
		mapReduceDriver.runTest();
		
		LOGGER.info("Logged");
	}

}
