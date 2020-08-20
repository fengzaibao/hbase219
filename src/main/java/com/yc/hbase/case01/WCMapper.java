package com.yc.hbase.case01;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * LongWritable 偏移量long 表示该行在文件中的位置，而不是行号
 * Text map阶段的输入数据一行文本信息 字符串类型String
 * Text map阶段的数据字符串类型String
 * IntWritable map阶段输出的value类型，对应java的int型，表示行号
 * @author ASUS
 *
 */
public class WCMapper extends Mapper<LongWritable, Text, Text,IntWritable> {
	/**
	 * key 输入的键
	 * value 输入的值
	 * context 上下文对象
	 */
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		String[] words=value.toString().split(" ");//分词
		for(String w:words){
			context.write(new Text(w), new IntWritable(1));
		}
	}

	
}
