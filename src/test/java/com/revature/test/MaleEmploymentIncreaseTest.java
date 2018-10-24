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

import com.revature.map.EmploymentMapperM;
import com.revature.reduce.EmploymentChangeReducerM;

public class MaleEmploymentIncreaseTest {

	// test Map, test Reduce, test Map-Reduce combo

	private MapDriver<LongWritable, Text, Text, DoubleWritable> mapDriver;
	private ReduceDriver<Text, DoubleWritable, Text, DoubleWritable> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable> mapReduceDriver;

	@Before
	public void setup(){
		EmploymentMapperM mapper = new EmploymentMapperM();
		mapDriver = new MapDriver<>();
		mapDriver.setMapper(mapper);

		EmploymentChangeReducerM reducer = new EmploymentChangeReducerM();
		reduceDriver = new ReduceDriver<>();
		reduceDriver.setReducer(reducer);
		//		
		mapReduceDriver = new MapReduceDriver();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}

	@Test
	public void testMapper(){
		Text test = new Text("\"United States\",\"WLD\",\"Gross graduation ratio, tertiary, male (%)\",\"SL.EMP.TOTL.SP.MA.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
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

		reduceDriver.withOutput(new Text("Increase in employed percentage of male poputaion since 2000:"), new DoubleWritable(-20.6));
		reduceDriver.withOutput(new Text("Increase in male employment since 2000 as a ratio:"), new DoubleWritable(-1.0));
		
		reduceDriver.runTest();
	}

	@Test
	public void testMapReducer(){
		Text test = new Text("\"United States\",\"WLD\",\"Gross graduation ratio, tertiary, male (%)\",\"SL.EMP.TOTL.SP.MA.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"20.6\",\"25.8\",\"28.6\",\"30.1\",\"35.9\",\"41.2\"");
		mapReduceDriver.withInput(new LongWritable(1), test);
		
		mapReduceDriver.withOutput(new Text("Increase in employed percentage of male poputaion since 2000:"), new DoubleWritable(-20.6));
		mapReduceDriver.withOutput(new Text("Increase in male employment since 2000 as a ratio:"), new DoubleWritable(-1.0));

		mapReduceDriver.runTest();
	}

}
