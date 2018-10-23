package com.revature.test;

import java.util.ArrayList;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.revature.map.USEmploymentMapperF;
import com.revature.reduce.EmploymentChangeReducerF;

public class FemaleEmploymentIncreaseTest {

	// test Map, test Reduce, test Map-Reduce combo

	private MapDriver<LongWritable, Text, Text, DoubleWritable> mapDriver;
	private ReduceDriver<Text, DoubleWritable, Text, DoubleWritable> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable> mapReduceDriver;

	@Before
	public void setup(){
		USEmploymentMapperF mapper = new USEmploymentMapperF();
		mapDriver = new MapDriver<>();
		mapDriver.setMapper(mapper);

		EmploymentChangeReducerF reducer = new EmploymentChangeReducerF();
		reduceDriver = new ReduceDriver<>();
		reduceDriver.setReducer(reducer);
		//		
		mapReduceDriver = new MapReduceDriver();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}

	@Test
	public void testMapper(){
		Text test = new Text("\"United States\",\"USA\",\"Gross graduation ratio, tertiary, female (%)\",\"SL.EMP.TOTL.SP.FE.NE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"20.6\",\"25.8\",\"28.6\",\"\",\"30.1\",\"35.9\",\"41.2\"");

		mapDriver.withInput(new LongWritable(1), test);

		//System.out.println(test.toString());

		mapDriver.withOutput(new Text("United States"), new DoubleWritable(20.6));
		mapDriver.withOutput(new Text("United States"), new DoubleWritable(25.8));
		mapDriver.withOutput(new Text("United States"), new DoubleWritable(28.6));
		mapDriver.withOutput(new Text("United States"), new DoubleWritable(30.1));
		mapDriver.withOutput(new Text("United States"), new DoubleWritable(35.9));
		mapDriver.withOutput(new Text("United States"), new DoubleWritable(41.2));
		
		mapDriver.runTest();
	}

	@Test
	public void testReducer(){
		ArrayList<DoubleWritable> values = new ArrayList<DoubleWritable>();
		values.add(new DoubleWritable(20.6));
		values.add(new DoubleWritable(25.8));
		values.add(new DoubleWritable(28.6));
		values.add(new DoubleWritable(30.1));
		values.add(new DoubleWritable(35.9));
		values.add(new DoubleWritable(41.2));

		reduceDriver.withInput(new Text("United States"), values);

		reduceDriver.withOutput(new Text("Increase in employed percentage of US female poputaion since 2000:"), new DoubleWritable(-20.6));
		reduceDriver.withOutput(new Text("Increase in US female employment since 2000 as a ratio:"), new DoubleWritable(-1.0));
		
		reduceDriver.runTest();
	}

	@Test
	public void testMapReducer(){
		Text test = new Text("\"United States\",\"USA\",\"Gross graduation ratio, tertiary, female (%)\",\"SL.EMP.TOTL.SP.FE.NE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"20.6\",\"25.8\",\"28.6\",\"30.1\",\"35.9\",\"41.2\"");
		mapReduceDriver.withInput(new LongWritable(1), test);
		
		mapReduceDriver.withOutput(new Text("Increase in employed percentage of US female poputaion since 2000:"), new DoubleWritable(-20.6));
		mapReduceDriver.withOutput(new Text("Increase in US female employment since 2000 as a ratio:"), new DoubleWritable(-1.0));

		mapReduceDriver.runTest();
	}

}
