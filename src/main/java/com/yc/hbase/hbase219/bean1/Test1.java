package com.yc.hbase.hbase219.bean1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class Test1 {
	public static void main(String[] args) {
		Configuration conf=new Configuration();
		conf.set("hbase.rootdir","hdfs://yc/hbase");
		//设置zk
		conf.set("hbase.zookeeper.quorum","node1,node2,node3");
		//设置zk端口号
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		
		//获取连接对象
		try {
			Connection conn=ConnectionFactory.createConnection(conf);
			System.out.println(conn.getClass().getName());
			Admin admin=conn.getAdmin();//create    drop    disable
			//创建TableName
			TableName tableName=TableName.valueOf("test01");
			//判断该表是否存在
			if(admin.tableExists(tableName)){
				System.out.println("表已存在");
			}else{
				//表不存在
				//创建表格描述器
				HTableDescriptor tableDescriptor=new HTableDescriptor(tableName);
				//创建列簇描述器对象 指定列簇名
				HColumnDescriptor columnDescriptor=new HColumnDescriptor("info");
				//添加列簇
				tableDescriptor.addFamily(columnDescriptor);
				admin.createTable(tableDescriptor);
				System.out.println("表创建成功!");
			}
			admin.close();
			conn.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
