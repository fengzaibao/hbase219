package com.yc.hbase.case01;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;



public class WcTest {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf=new Configuration();
		conf.set("hbase.rootdir","hdfs://yc/hbase");
		//设置zk
		conf.set("hbase.zookeeper.quorum","node1,node2,node3");
		//设置zk端口号
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		Job job=Job.getInstance(conf);
		job.setJarByClass(WcTest.class);
		job.setMapperClass(WCMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		TableMapReduceUtil.initTableReducerJob("wc_01", WCReducer.class,job);
		Path path=new Path("hdfs://node1:8020/wordcount/input/README.txt");
		FileInputFormat.addInputPath(job, path);
		boolean flag=job.waitForCompletion(true);
		if(flag){
			System.out.println("success");
		}
	}
}
