package com.yc.hbase.case02;

import java.io.IOException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Mapper;

public class ReadFruitMapper extends TableMapper<ImmutableBytesWritable,Put> {
	@Override
	protected void map(ImmutableBytesWritable key, Result value,Context context)
			throws IOException, InterruptedException {
		//fruit表中的info列族      name和color提取出来,相当于将每一行的数据读取出来放在Put对象中
		Put put=new Put(key.get());
		for(Cell cell:value.rawCells()){
			if("info".equals(Bytes.toString(CellUtil.cloneFamily(cell)))){
				if("name".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))){
					put.add(cell);
				}else if("color".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))){
					put.add(cell);
				}
			}
		}
		//将Put写入到context中作为map输出
		context.write(key, put);
	}	
}
