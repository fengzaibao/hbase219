package com.yc.hbase.case02;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class Test {
	
	public static void main(String[] args) throws Exception {
		Configuration conf=new Configuration();
		conf.set("hbase.rootdir","hdfs://yc/hbase");
		//设置zk
		conf.set("hbase.zookeeper.quorum","node1,node2,node3");
		//设置zk端口号
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		int status=ToolRunner.run(conf, new FruitMapReducer(),args);
		System.exit(status);
		
	}
}
