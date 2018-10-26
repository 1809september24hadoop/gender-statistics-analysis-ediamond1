package com.revature.util;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Partitions input based on whether or not female population ratio is greater than 50%
 * @author cloudera
 *
 */
public class FPopPartitioner extends
Partitioner < Text, DoubleWritable >
{
	@Override
	public int getPartition(Text key, DoubleWritable value, int numReduceTasks){
		
		if(numReduceTasks == 0){
			return 0;
		}

		if(value.get()<=50){
			return 0 % numReduceTasks;
		}else{
			return 1 % numReduceTasks;
		}
	}
}
