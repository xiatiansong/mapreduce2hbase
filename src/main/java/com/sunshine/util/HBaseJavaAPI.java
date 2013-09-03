package com.sunshine.util;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 操作hbase的工具类
 * 
 * @author hadoop
 * 
 */
public class HBaseJavaAPI {
	// 声明静态配置
	private static Configuration conf = null;
	static {
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "master");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
	}

	/**
	 * 创建数据库表
	 * 
	 * @param tableName-表名
	 * @param columnFamilys-列族数组
	 * @throws Exception
	 */
	public static void createTable(String tableName, String[] columnFamilys) throws Exception {
		// 新建一个数据库管理员
		HBaseAdmin hAdmin = new HBaseAdmin(conf);
		if (hAdmin.tableExists(tableName)) {
			System.out.println("表已经存在");
			System.exit(0);
		} else {
			// 新建一个 scores 表的描述
			HTableDescriptor tableDesc = new HTableDescriptor(tableName);
			// 在描述里添加列族
			for (String columnFamily : columnFamilys) {
				tableDesc.addFamily(new HColumnDescriptor(columnFamily));
			}
			// 根据配置好的描述建表
			hAdmin.createTable(tableDesc);
			System.out.println("创建表成功");
		}
	}

	/**
	 * 删除数据库表
	 * @param tableName-hbase表名
	 * @throws Exception
	 */
	public static void deleteTable(String tableName) throws Exception {
		// 新建一个数据库管理员
		HBaseAdmin hAdmin = new HBaseAdmin(conf);
		if (hAdmin.tableExists(tableName)) {
			// 关闭一个表
			hAdmin.disableTable(tableName);
			// 删除一个表
			hAdmin.deleteTable(tableName);
			System.out.println("删除表成功");
		} else {
			System.out.println("删除的表不存在");
			System.exit(0);
		}
	}

	/**
	 * 添加一条数据
	 * 
	 * @param tableName-表名
	 * @param rowKey-行键
	 * @param columnFamily-列族名
	 * @param column-列名
	 * @param value-值
	 * @throws Exception
	 */
	public static void addRow(String tableName, String rowKey, String columnFamily, String column, String value) throws Exception {
		HTable table = new HTable(conf, tableName);
		Put put = new Put(Bytes.toBytes(rowKey));
		// 参数出分别：列族、列、值
		put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
		table.put(put);
	}

	/**
	 * 删除一条数据
	 * 
	 * @param tableName-表名
	 * @param rowKey-行键
	 * @throws Exception
	 */
	public static void delRow(String tableName, String rowKey) throws Exception {
		HTable table = new HTable(conf, tableName);
		Delete del = new Delete(Bytes.toBytes(rowKey));
		table.delete(del);
	}

	/**
	 * 删除多条数据
	 * @param tableName-表名
	 * @param rowKeys-行键数组
	 * @throws Exception
	 */
	public static void delMultiRows(String tableName, String[] rowKeys) throws Exception {
		HTable table = new HTable(conf, tableName);
		List<Delete> list = new ArrayList<Delete>();
		for (String rowKey : rowKeys) {
			Delete del = new Delete(Bytes.toBytes(rowKey));
			list.add(del);
		}
		table.delete(list);
	}

	/**
	 * 获取一条数据
	 * @param tableName-表名
	 * @param rowKey-行键数组
	 * @throws Exception
	 */
	public static void getRow(String tableName, String rowKey) throws Exception {
		HTable table = new HTable(conf, tableName);
		Get get = new Get(Bytes.toBytes(rowKey));
		Result result = table.get(get);
		// 输出结果
		for (KeyValue rowKV : result.raw()) {
			System.out.print("行名：" + new String(rowKV.getRow()) + " ");
			System.out.print("时间戳：" + rowKV.getTimestamp() + " ");
			System.out.print("列族名：" + new String(rowKV.getFamily()) + " ");
			System.out.print("列名：" + new String(rowKV.getQualifier()) + " ");
			System.out.println("值：" + new String(rowKV.getValue()) + " ");
		}
	}

	/**
	 * 获取所有数据
	 * @param tableName-表名
	 * @throws Exception
	 */
	public static void getAllRows(String tableName) throws Exception {
		HTable table = new HTable(conf, tableName);
		Scan scan = new Scan();
		ResultScanner results = table.getScanner(scan);
		//scan.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("attr"));
		//scan.setStartRow( Bytes.toBytes("row"));                   // start key is inclusive
		// scan.setStopRow( Bytes.toBytes("row" + (char)0)); // stop key is  exclusive
		// 输出结果
		for (Result result : results) {
			//for (Result r = rs.next(); r != null; r = rs.next()) {
			for (KeyValue rowKV : result.raw()) {
				System.out.print("行名：" + new String(rowKV.getRow()) + " ");
				System.out.print("时间戳：" + rowKV.getTimestamp() + " ");
				System.out.print("列族名：" + new String(rowKV.getFamily()) + " ");
				System.out.print("列名：" + new String(rowKV.getQualifier()) + " ");
				System.out.println("值：" + new String(rowKV.getValue()) + " ");
			}
		}
	}

	// 主函数
	public static void main(String[] args) {
		try {
			String tableName = "student";
			// 第一步：创建数据库表：“student”
			String[] columnFamilys = { "info", "course" };
			HBaseJavaAPI.createTable(tableName, columnFamilys);
			// 第二步：向数据表的添加数据
			// 添加第一行数据
			HBaseJavaAPI.addRow(tableName, "xiapi", "info", "age", "20");
			HBaseJavaAPI.addRow(tableName, "xiapi", "info", "sex", "boy");
			HBaseJavaAPI.addRow(tableName, "xiapi", "course", "china", "97");
			HBaseJavaAPI.addRow(tableName, "xiapi", "course", "math", "128");
			HBaseJavaAPI.addRow(tableName, "xiapi", "course", "english", "85");
			// 添加第二行数据
			HBaseJavaAPI.addRow(tableName, "xiaoxue", "info", "age", "19");
			HBaseJavaAPI.addRow(tableName, "xiaoxue", "info", "sex", "boy");
			HBaseJavaAPI.addRow(tableName, "xiaoxue", "course", "china", "90");
			HBaseJavaAPI.addRow(tableName, "xiaoxue", "course", "math", "120");
			HBaseJavaAPI.addRow(tableName, "xiaoxue", "course", "english", "90");
			// 添加第三行数据
			HBaseJavaAPI.addRow(tableName, "qingqing", "info", "age", "18");
			HBaseJavaAPI.addRow(tableName, "qingqing", "info", "sex", "girl");
			HBaseJavaAPI.addRow(tableName, "qingqing", "course", "china", "100");
			HBaseJavaAPI.addRow(tableName, "qingqing", "course", "math", "100");
			HBaseJavaAPI.addRow(tableName, "qingqing", "course", "english", "99");
			// 第三步：获取一条数据
			System.out.println("获取一条数据");
			HBaseJavaAPI.getRow(tableName, "xiapi");
			// 第四步：获取所有数据
			System.out.println("获取所有数据");
			HBaseJavaAPI.getAllRows(tableName);
			// 第五步：删除一条数据
			System.out.println("删除一条数据");
			HBaseJavaAPI.delRow(tableName, "xiapi");
			HBaseJavaAPI.getAllRows(tableName);
			// 第六步：删除多条数据
			System.out.println("删除多条数据");
			String[] rows = { "xiaoxue", "qingqing" };
			HBaseJavaAPI.delMultiRows(tableName, rows);
			HBaseJavaAPI.getAllRows(tableName);
			// 第八步：删除数据库
			System.out.println("删除数据库");
			HBaseJavaAPI.deleteTable(tableName);
		} catch (Exception err) {
			err.printStackTrace();
		}
	}
}