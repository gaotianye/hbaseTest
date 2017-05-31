package cn.gaotianye.hbase.crxy;

import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseCRUD {
	private static HBaseAdmin hBAdmin;
	private static Configuration conf = new Configuration();
	
	static{
		conf.set("hbase.zookeeper.quorum", "master,slave1,slave2");
		try {
			hBAdmin = new HBaseAdmin(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) throws Exception {
		String tableName = "tb_5";
		String familyName = "cf1";
		String rowKey = "201705311544";
		String columnName = "scores";
		String columnName2 = "nums";
		String columnValue = "70";
		String columnValue2 = "1002901";
		
		//创建表
		createTable(tableName,familyName);
		//展示表
		listTable();
		//往表中添加数据
		for(int i = 0;i<10;i++){
			put(tableName, rowKey+"_"+i, familyName, columnName, Integer.parseInt(columnValue)+i+"");
		}
		for(int i = 5;i<10;i++){
			put(tableName, rowKey+"_"+i, familyName, columnName2, Integer.parseInt(columnValue2)+i+"");
		}
		//根据rowKey查询数据
		selectByRowKey(tableName, rowKey+"_4");
		//查询所有数据
		selectAll(tableName);
		//删除指定的rowKey信息
		deleteByRowKey(tableName, rowKey+"_8");
		selectAll(tableName);
		//根据指定条件查询
		//TODO 不好使
		selectByQuery(tableName, familyName, columnName2, CompareOp.GREATER_OR_EQUAL, "74");
		//清理工作
		cleanUp();
	}
	
	/**
	 * 清理工作
	 * @throws Exception
	 */
	public static void cleanUp() throws Exception{
		System.out.println("--------清理工作--------");
		hBAdmin.close();
	}
	
	/**
	 * 展示所有table
	 * @throws Exception
	 */
	public static void listTable() throws Exception{
		System.out.println("--------list Table:--------");
		String[] tbls = hBAdmin.getTableNames();
		System.out.println(Arrays.toString(tbls));
		TableName[] tbls2 = hBAdmin.listTableNames();
		System.out.println(Arrays.toString(tbls2));
		HTableDescriptor[] listTables = hBAdmin.listTables();
		for (HTableDescriptor hTDesc : listTables) {
			TableName tblName = hTDesc.getTableName();
			System.out.print(tblName + " ");
		}
		System.out.println();
	}
	
	/**
	 * 创建表  
	 * create 'tableName','familyName',....
	 * @throws Exception
	 */
	public static void createTable(String tableName,String... familyNames) throws Exception{
		System.out.println("--------create Table:--------");
		//如果表存在，先删除，再创建
		if(hBAdmin.tableExists(tableName)){
			hBAdmin.disableTable(tableName);
			hBAdmin.deleteTable(tableName);
		}
		HTableDescriptor desc = new HTableDescriptor(tableName);
		for (String familyName : familyNames) {
			desc.addFamily(new HColumnDescriptor(familyName));
		}
		hBAdmin.createTable(desc);
	}
	
	/**
	 * put 操作（只有一个列族和一个列）
	 * put "tableName","rowKey","FamilyName:columnName","columnValue"
	 * @param tableName
	 * @param rowKey
	 * @param FamilyName
	 * @param columnName
	 * @param columnValue
	 * @throws Exception
	 */
	public static void put(String tableName,String rowKey,String FamilyName,String columnName,String columnValue) throws Exception{
		System.out.println("-------------put start----------------");
		HTable table = new HTable(conf, tableName);
		Put put = new Put(rowKey.getBytes());
		put.add(FamilyName.getBytes(), columnName.getBytes(),columnValue.getBytes());
		table.put(put);
		System.out.println("-------------put end----------------");
	}
	
	/**
	 * 查询执行rowKey的信息
	 * get 'tableName','rowKey'
	 * @param rowKey
	 * @throws Exception
	 */
	public static void selectByRowKey(String tableName,String rowKey) throws Exception{
		System.out.println("-------------查询指定 rowkey信息 start---------------");
		HTable table = new HTable(conf, tableName);
		Get get = new Get(rowKey.getBytes());
		Result result = table.get(get);
		KeyValue[] keyValues = result.raw();
		for (KeyValue kv : keyValues) {
			System.out.println(new String(kv.getQualifier()) + "=" + new String(kv.getValue()));
		}
		System.out.println("-------------查询指定 rowkey信息 end---------------");
	}
	/**
	 * 查询table表的所有信息
	 * scan 'tableName'
	 * @param tableName
	 * @throws Exception
	 */
	public static void selectAll(String tableName) throws Exception{
		System.out.println("---------------------查询table表的所有信息  start----------------------");
		HTable table = new HTable(conf, tableName);
		Scan scan = new Scan();
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			System.out.print(new String(result.getRow()) + "\t");
			for(Cell cell : result.rawCells()) {
				System.out.print(new String(cell.getQualifier()) + "=" + new String(cell.getValue()) + "\t");
			}
			System.out.println();
		}
		System.out.println("---------------------查询table表的所有信息  end----------------------");
	}
	/**
	 * 根据过滤条件查询
	 * （1）ok
	 * scan 'tableName',{FILTER => "(PrefixFilter ('rowKey'))"}
	 * 结果：
	 *  ROW                                         COLUMN+CELL                                                                                                                    
 		201705311452_5                             column=cf1:nums, timestamp=1496214978608, value=1002906                                                                        
 		201705311452_5                             column=cf1:scores, timestamp=1496214978587, value=75 
	 * （2）QualifierFilter执行没有什么效果
	 * scan 'tb_3',{FILTER => "(PrefixFilter ('201705311452_5') AND (QualifierFilter (>=, 'binary:76')))"}
	 * （3）ok
	 * get 'tableName','rowKey',{FILTER => "ValueFilter(=, 'binary:columnValue')"}
	 * @param tableName 表
	 * @param familyName 列族
	 * @param columnName 列
	 * @param compareOp 条件
	 * @param scores 值
	 * @throws Exception
	 */
	public static void selectByQuery(String tableName,String familyName,String columnName,CompareOp compareOp,String scores) throws Exception{
		System.out.println("-----------根据条件查询 start------------");
		HTable table = new HTable(conf, tableName);
		Scan scan = new Scan();
		scan.addColumn(familyName.getBytes(), columnName.getBytes());//加入了列的过滤，显示满足该列的条件
		Filter filter = new SingleColumnValueFilter(//加入了过滤器，显示满足该过滤器条件
						familyName.getBytes(), columnName.getBytes(), 
						compareOp, Bytes.toBytes(scores));
		scan.setFilter(filter);
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			System.out.print(new String(result.getRow()) + "\t");
			for(Cell cell : result.rawCells()) {
				System.out.print(new String(cell.getQualifier()) + "=" + new String(cell.getValue()) + "\t");
			}
			System.out.println();
		}
		System.out.println("-----------根据条件查询 end------------");
	}
	
	/**
	 * 删除指定的rowKey信息(java代码中可以不指定列族和列，但是shell不可以)
	 * delete 'tableName','rowKey','familyName:columnName'
	 * @param tableName
	 * @param rowKey
	 * @throws Exception
	 */
	public static void deleteByRowKey(String tableName,String rowKey) throws Exception{
		System.out.println("------------删除指定的rowKey信息 start--------------------");
		HTable table = new HTable(conf, tableName);
		Delete delete = new Delete(rowKey.getBytes());
		table.delete(delete);
		System.out.println("------------删除指定的rowKey信息 end--------------------");
	}
}
