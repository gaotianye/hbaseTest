package cn.gaotianye.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class Hbase3 {
	HBaseAdmin hbaseAdmin = null;
	HTable hTable = null;
	String tb_name = "tb1";
	String family_name = "cf1";
	
	/**
	 * TODO 插入数据
	 * @throws Exception
	 */
	@Test
	public void insert() throws Exception{
		String rowKey = "";
		Put put = new Put(rowKey.getBytes());
		hTable.put(put);
	}
	
	/**
	 * 创建表
	 * @throws Exception
	 */
	@Test
	public void CreateTb() throws Exception{
		if(hbaseAdmin.tableExists(tb_name)){
			hbaseAdmin.disableTable(tb_name);
			hbaseAdmin.deleteTable(tb_name);
		}
		HTableDescriptor desc = new HTableDescriptor(tb_name);
		
		HColumnDescriptor family = new HColumnDescriptor(family_name);
		
		family.setBlockCacheEnabled(true);
		family.setInMemory(true);
		family.setMaxVersions(1);
		
		desc.addFamily(family);
		
		hbaseAdmin.createTable(desc);
	}
	
	@Before
	public void begin() throws Exception{
		Configuration config = new Configuration();
		config.set("hbase.zookeeper.quorum", "mmm,slave1,slave2");
		hbaseAdmin = new HBaseAdmin(config);
		hTable = new HTable(config, tb_name);
	}
	
	@After
	public void end() throws Exception{
		if(hbaseAdmin!=null){
			hbaseAdmin.close();
		}
		if(hTable!=null){
			hTable.close();
		}
	}
}
