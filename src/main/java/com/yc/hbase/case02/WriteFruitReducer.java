package com.yc.hbase.case02;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.NullWritable;

public class WriteFruitReducer extends 
TableReducer<ImmutableBytesWritable,Put,NullWritable>{

	@Override
	protected void reduce(ImmutableBytesWritable key, Iterable<Put> values,Context context)
			throws IOException, InterruptedException {
		//读取一行写入fruit_01  mr
		for(Put put:values){
			context.write(NullWritable.get(), put);
		}
	}
}
