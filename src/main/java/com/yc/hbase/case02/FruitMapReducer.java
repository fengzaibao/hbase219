package com.yc.hbase.case02;

import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
/**
 * 组装job
 */
public class FruitMapReducer extends Configured implements Tool{

	@Override
	public int run(String[] args) throws Exception {
		//获取配置对象
		Configuration conf=this.getConf();
		//创建job任务
		Job job=Job.getInstance(conf,this.getClass().getSimpleName());
		job.setJarByClass(FruitMapReducer.class);
		Scan scan=new Scan();
		scan.setCacheBlocks(false);
		TableMapReduceUtil.initTableMapperJob("fruit_01",//数据源
				scan,//扫描器对象
				ReadFruitMapper.class,//设置Mapper类 
				ImmutableBytesWritable.class, //输出Key类型
				Put.class,//设置Mapper输出Value类型
				job);//设置给那个job
		TableMapReduceUtil.initTableReducerJob("fruit_01_mr", WriteFruitReducer.class, job);
		
		job.setNumReduceTasks(1);
		boolean flag=job.waitForCompletion(true);
		if(!flag){
			throw new IOException("job running with error ");
		}
		return flag?0:1;
	}
	
	
}
