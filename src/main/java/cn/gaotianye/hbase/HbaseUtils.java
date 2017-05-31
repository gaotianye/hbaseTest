package cn.gaotianye.hbase;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class HbaseUtils {
	static HBaseAdmin hbaseAdmin = null;
	
	public static void main(String[] args) {
		createTb("phone", "cf");
	}
	
	private static void createTb(String tb_name,String family_name) {
		System.out.println("create table start");
		
		Configuration config = new Configuration();
		config.set("hbase.zookeeper.quorum", "mmm,slave1,slave2");
		try {
			hbaseAdmin = new HBaseAdmin(config);
		}catch (Exception e) {
			e.printStackTrace();
		}
		try {
			if(hbaseAdmin.tableExists(tb_name)){
				hbaseAdmin.disableTable(tb_name);
				hbaseAdmin.deleteTable(tb_name);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		HTableDescriptor desc = new HTableDescriptor(tb_name);
		
		HColumnDescriptor family = new HColumnDescriptor(family_name);
		
		family.setBlockCacheEnabled(true);
		family.setInMemory(true);
		family.setMaxVersions(1);
		
		desc.addFamily(family);
		
		try {
			hbaseAdmin.createTable(desc);
			System.out.println("create table end");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
