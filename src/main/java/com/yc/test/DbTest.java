package com.yc.test;

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.Test;

import com.yc.hbase.hbase219.bean1.DbHelper;

public class DbTest {
	DbHelper db=new DbHelper();
	@Test
	public void test() throws IOException {
		System.out.println(db.deleteTable("test01"));
	}
	@Test
	public void testScannTable() throws IOException {
		db.scanTable("fengzaibao");
	}
	@Test
	public void testcreate() throws IOException {
		System.out.println(db.createTable("shop","info"));
	}
}
