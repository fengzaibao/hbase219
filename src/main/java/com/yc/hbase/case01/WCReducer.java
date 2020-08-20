package com.yc.hbase.case01;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
/**
 * Text 数据类型:字符串类型String
 * IntWritable reduce阶段输入类型int
 * Text reduce阶段的输出数据类型String类型
 * IntWritable 输出词频个数int型
 * @author ASUS
 *
 */
public class WCReducer extends TableReducer<Text,IntWritable,ImmutableBytesWritable> {
	/**
	 * key 输入的键
	 * value 输入的值
	 * context 上下文对象，用于输出键值对
	 */
	@Override
	protected void reduce(Text text, Iterable<IntWritable> iterable,Context context)
			throws IOException, InterruptedException {
		int sum=0;
		for(IntWritable i:iterable){
			sum+=i.get();
		}
		if("".equals(text.toString())){
			return;
		}
		Put put=new Put(text.toString().getBytes());//单词作为rowkey
		//列族名cf  列名:count   值 数量  字节数组
		put.add("cf".getBytes(),"count".getBytes(),(sum+"").getBytes());
		//单词  个数 hadoop
		context.write(null, put);
	}

	
	
	
}
