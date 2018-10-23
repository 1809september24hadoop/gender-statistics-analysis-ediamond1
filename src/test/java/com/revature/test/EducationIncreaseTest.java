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

import com.revature.map.USGraduationMapperF;
import com.revature.reduce.EducationIncreaseFReducer;
import com.revature.reduce.IncreaseAvgReducer;

public class EducationIncreaseTest {

	// test Map, test Reduce, test Map-Reduce combo

	private MapDriver<LongWritable, Text, Text, DoubleWritable> mapDriver;
	private ReduceDriver<Text, DoubleWritable, Text, DoubleWritable> combineDriver;
	private ReduceDriver<Text, DoubleWritable, Text, DoubleWritable> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable> mapReduceDriver;

	@Before
	public void setup(){
		USGraduationMapperF mapper = new USGraduationMapperF();
		mapDriver = new MapDriver<>();
		mapDriver.setMapper(mapper);

		EducationIncreaseFReducer combiner = new EducationIncreaseFReducer();
		combineDriver = new ReduceDriver<>();
		combineDriver.setReducer(combiner);

		IncreaseAvgReducer reducer = new IncreaseAvgReducer();
		reduceDriver = new ReduceDriver<>();
		reduceDriver.setReducer(reducer);
		//		
		mapReduceDriver = new MapReduceDriver();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setCombiner(combiner);
		mapReduceDriver.setReducer(reducer);
	}

	@Test
	public void testMapper(){
		Text test = new Text("\"United States\",\"USA\",\"Gross graduation ratio, tertiary, female (%)\",\"SE.TER.CMPL.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"20.6\",\"25.8\",\"28.6\",\"\",\"30.1\",\"35.9\"");

		mapDriver.withInput(new LongWritable(1), test);

		//System.out.println(test.toString());

		mapDriver.withOutput(new Text("United States"), new DoubleWritable(20.6));
		mapDriver.withOutput(new Text("United States"), new DoubleWritable(25.8));
		mapDriver.withOutput(new Text("United States"), new DoubleWritable(28.6));
		mapDriver.withOutput(new Text("United States"), new DoubleWritable(30.1));
		mapDriver.withOutput(new Text("United States"), new DoubleWritable(35.9));

		mapDriver.runTest();
	}

	@Test
	public void testCombiner(){
		ArrayList<DoubleWritable> values = new ArrayList<DoubleWritable>();
		values.add(new DoubleWritable(20.6));
		values.add(new DoubleWritable(25.8));
		values.add(new DoubleWritable(28.6));
		values.add(new DoubleWritable(30.1));
		values.add(new DoubleWritable(35.9));

		combineDriver.withInput(new Text("United States"), values);

		combineDriver.withOutput(new Text("increase"), new DoubleWritable(5.2));
		combineDriver.withOutput(new Text("increase"), new DoubleWritable(2.8));
		combineDriver.withOutput(new Text("increase"), new DoubleWritable(1.5));
		combineDriver.withOutput(new Text("increase"), new DoubleWritable(5.8));

		combineDriver.runTest();
	}

	@Test
	public void testReducer(){
		ArrayList<DoubleWritable> values = new ArrayList<DoubleWritable>();
		values.add(new DoubleWritable(5.2));
		values.add(new DoubleWritable(2.8));
		values.add(new DoubleWritable(1.5));
		values.add(new DoubleWritable(5.8));

		reduceDriver.withInput(new Text("increase"), values);

		reduceDriver.withOutput(new Text("Average yearly increase in US Female Graduation since 2000:"), new DoubleWritable(3.825));

		reduceDriver.runTest();
	}

	@Test
	public void testMapReducer(){
		Text test = new Text("\"United States\",\"USA\",\"Gross graduation ratio, tertiary, female (%)\",\"SE.TER.CMPL.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"20.6\",\"25.8\",\"28.6\",\"30.1\",\"35.9\"");
		mapReduceDriver.withInput(new LongWritable(1), test);
		
		mapReduceDriver.withOutput(new Text("Average yearly increase in US Female Graduation since 2000:"), new DoubleWritable(3.825));

		mapReduceDriver.runTest();
	}

}
