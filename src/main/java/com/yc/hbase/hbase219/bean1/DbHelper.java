package com.yc.hbase.hbase219.bean1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class DbHelper {
	private static Configuration conf;
	static{
		conf=new Configuration();
		conf.set("hbase.rootdir","hdfs://yc/hbase");
		//设置zk
		conf.set("hbase.zookeeper.quorum","node1,node2,node3");
		//设置zk端口号
		conf.set("hbase.zookeeper.property.clientPort", "2181");
	}
	/**
	 * 判断表是否存在
	 * @return
	 * @throws IOException 
	 */
	public  boolean isExitsTable(String tname) throws IOException{
		Connection conn=ConnectionFactory.createConnection(conf);
		Admin admin=conn.getAdmin();//create    drop    disable
		//创建TableName
		TableName tableName=TableName.valueOf(tname);
		//判断该表是否存在
		if(admin.tableExists(tableName)){
			return true;
		}
		return false;
	}
	/**
	 * 
	 * @param tname表名
	 * @param columnFamily 列族
	 * @throws IOException
	 */
	public  boolean createTable(String tname,String...columnFamily)throws IOException{
		if(isExitsTable(tname)){
			System.out.println("表已存在..");
			return false;
		}
		Connection conn=ConnectionFactory.createConnection(conf);
		Admin admin=conn.getAdmin();//create    drop    disable
		//创建表格描述器
		HTableDescriptor tableDescriptor=new HTableDescriptor(tname);
		//创建列族描述器对象 指定列族名
		for(String family:columnFamily){
			HColumnDescriptor columnDescriptor=new HColumnDescriptor(family);
			//添加列簇
			tableDescriptor.addFamily(columnDescriptor);
		}
		//创建表
		admin.createTable(tableDescriptor);
		System.out.println("表创建成功!");
		admin.close();
		conn.close();
		return true;
	}
	/**
	 * 删除表 需要判断是不是disable，是才能删除
	 * @param tname
	 * @throws IOException
	 */
	public  boolean deleteTable(String tname)throws IOException{
		if(!isExitsTable(tname)){
			System.out.println("表不存在..");
			return false;
		}
		Connection conn=ConnectionFactory.createConnection(conf);
		Admin admin=conn.getAdmin();//create    drop    disable
		if(!admin.isTableDisabled(TableName.valueOf(tname))){
			admin.disableTable(TableName.valueOf(tname));
		}
		//删除表
		admin.deleteTable(TableName.valueOf(tname));
		admin.close();
		conn.close();
		return true;
	}
	/**
	 *下面的方法完成添加一条数据,从已经创建的表中
	 * @param tname表名
	 * @param rowKey行键
	 * @param cf列族
	 * @param column列名
	 * @param value值
	 * @throws IOException 
	 */
	public void addRow(String tname,String rowKey,String cf,String column,String value) throws IOException{
		Connection conn=ConnectionFactory.createConnection(conf);
		Table table=conn.getTable(TableName.valueOf(tname));
		if(!isExitsTable(tname)){
			System.out.println("表不存在..");
			boolean b=createTable(tname);
			if(b){
				//创建put对象用于添加数据
				Put put=new Put(rowKey.getBytes());//传进来rowKey
				put.addColumn(cf.getBytes(), column.getBytes(), value.getBytes());//String.getBytes转为byte[]
				table.put(put);
				table.close();
				conn.close();
			}else{
				return;
			}
		}
	}
	/**
	 * 批量处理,用List多量传输
	 * 只是对一张表进行多量数据插入
	 * @throws IOException 
	 */
	public boolean ddMangRow(String tname,List<Map<String,String>> params) throws IOException{
		Connection conn=ConnectionFactory.createConnection(conf);
		Table table=conn.getTable(TableName.valueOf(tname));
		//创建put对象用于添加数据
		if(params==null||params.isEmpty()){
			return false;
		}
		List<Put> puts=new ArrayList<Put>();
		for(Map<String,String> map:params){
			Put put=new Put(map.get("rowKey").getBytes());//传进来rowKey
			put.addColumn(map.get("cf").getBytes(), map.get("column").getBytes(),map.get("value").getBytes());//String.getBytes转为byte[]
			puts.add(put);
		}
		table.put(puts);
		table.close();
		conn.close();
		return true;
	}
	/**
	 * 根据行键,删除数据
	 * @param tname
	 * @param rowKey
	 * @throws IOException
	 */
	public void deleteRow(String tname,String rowKey) throws IOException{
		Connection conn=ConnectionFactory.createConnection(conf);
		Table table=conn.getTable(TableName.valueOf(tname));
		Delete delete=new Delete(rowKey.getBytes());
		table.delete(delete);
		table.close();
		conn.close();
	}
	/**
	 * 全盘扫描
	 * @param tname
	 * @throws IOException
	 */
	public void scanTable(String tname) throws IOException{
		Connection conn=ConnectionFactory.createConnection(conf);
		Table table=conn.getTable(TableName.valueOf(tname));
		Scan scan=new Scan();
		ResultScanner resultScanner=table.getScanner(scan);
		for(Result result:resultScanner){
			//byte[] rowKey=result.getRow();//获取行键
			//获取所有的单元格,就是每一列的东西(cell)
			Cell[] cells=result.rawCells();
			for(Cell cell:cells){
				System.out.println("行键:"+Bytes.toString(CellUtil.cloneRow(cell)));
				System.out.println("列族名:"+Bytes.toString(CellUtil.cloneFamily(cell)));
				System.out.println("列名:"+Bytes.toString(CellUtil.cloneQualifier(cell)));
				System.out.println("值:"+Bytes.toString(CellUtil.cloneValue(cell)));
			}
		}
		table.close();
		conn.close();
	}
}
