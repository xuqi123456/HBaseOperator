package org.bigdata;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor.Builder;
import org.apache.hadoop.hbase.NamespaceExistException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.NamespaceDescriptorOrBuilder;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author Xu Qi
 * @since 2022/10/25
 */
public class TestAPI {

  public static Connection connection;
  private static final Admin admin;

  static {
//使用 HBaseConfiguration 的单例方法实例化
    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum", "localhost");
    conf.set("hbase.zookeeper.property.clientPort", "2181");
    try {
      connection = ConnectionFactory.createConnection(conf);
      admin = connection.getAdmin();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static void close() {
    if (admin != null) {
      try {
        admin.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    if (connection != null) {
      try {
        connection.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static boolean isTableExist(String tableName) throws IOException {
    return admin.tableExists(TableName.valueOf(tableName));
  }

  public static void createTable(String tableName, String...
      columnFamily) throws IOException {
    //判断表是否存在
    if (isTableExist(tableName)) {
      System.out.println("表" + tableName + "已存在");
    } else {
      //创建表属性对象,表名需要转字节
      HTableDescriptor descriptor = new
          HTableDescriptor(TableName.valueOf(tableName));
      //创建多个列族
      for (String cf : columnFamily) {
        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
        //在列修饰符中可指定存储的版本数
        hColumnDescriptor.setMaxVersions(3);
        descriptor.addFamily(hColumnDescriptor);
      }
      //根据对表的配置，创建表
      admin.createTable(descriptor);
      System.out.println("表" + tableName + "创建成功！ ");
    }
  }

  public static void dropTable(String tableName) throws IOException {
    if (isTableExist(tableName)) {
      admin.disableTable(TableName.valueOf(tableName));
      admin.deleteTable(TableName.valueOf(tableName));
      System.out.println("表" + tableName + "删除成功！ ");
    } else {
      System.out.println("表" + tableName + "不存在！ ");
    }
  }

  public static void createNameSpace(String ns) {
    Builder builder = NamespaceDescriptor.create(ns);
    NamespaceDescriptor build = builder.build();
    try {
      admin.createNamespace(build);
    } catch (NamespaceExistException e) {
      System.out.println("命名空间已经存在");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    System.out.println("会执行到这");
  }

  public static void addRowData(String tableName, String rowKey,
      String columnFamily, String
      column, String value) throws IOException {
    Table table = connection.getTable(TableName.valueOf(tableName));
//    //创建 HTable 对象
//    HTable hTable = new HTable(conf, tableName);
  //向表中插入数据
    Put put = new Put(Bytes.toBytes(rowKey));
  //向 Put 对象中组装数据
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column),
        Bytes.toBytes(value));
    table.put(put);
    table.close();
//    hTable.put(put);
//    hTable.close();
    System.out.println("插入数据成功");
  }

  public static void getRowQualifier(String tableName, String rowKey,
      String family, String
      qualifier) throws IOException{
    Table table = connection.getTable(TableName.valueOf(tableName));
    Get get = new Get(Bytes.toBytes(rowKey));
    get.addColumn(Bytes.toBytes(family),
        Bytes.toBytes(qualifier));
    get.setMaxVersions();
    Result result = table.get(get);
    for(Cell cell : result.rawCells()){
      System.out.println(" 行 键 :" +
          Bytes.toString(result.getRow()));
      System.out.println(" 列 族 " +
          Bytes.toString(CellUtil.cloneFamily(cell)));
      System.out.println(" 列 :" +
          Bytes.toString(CellUtil.cloneQualifier(cell)));
      System.out.println(" 值 :" +
          Bytes.toString(CellUtil.cloneValue(cell)));
    }
  }

  public static void getRow(String tableName, String rowKey) throws
      IOException{
    Table table = connection.getTable(TableName.valueOf(tableName));
    Get get = new Get(Bytes.toBytes(rowKey));
    //get.setMaxVersions();显示所有版本
    //get.setTimeStamp();显示指定时间戳的版本
    Result result = table.get(get);
    for(Cell cell : result.rawCells()){
      System.out.println(" 行 键 :" +Bytes.toString(result.getRow()));
      System.out.println(" 列 族 " +
          Bytes.toString(CellUtil.cloneFamily(cell)));
      System.out.println(" 列 :" +
          Bytes.toString(CellUtil.cloneQualifier(cell)));
      System.out.println(" 值 :" +
          Bytes.toString(CellUtil.cloneValue(cell)));
      System.out.println("时间戳:" + cell.getTimestamp());
    }
  }

  public static void getAllRows(String tableName) throws IOException{
    Table table = connection.getTable(TableName.valueOf(tableName));
    //得到用于扫描 region 的对象
    Scan scan = new Scan(Bytes.toBytes("1001"),Bytes.toBytes("1002"));
    //使用 HTable 得到 resultcanner 实现类的对象
    ResultScanner resultScanner = table.getScanner(scan);
    for(Result result : resultScanner){
      Cell[] cells = result.rawCells();
      for(Cell cell : cells){
    //得到 rowkey
        System.out.println(" 行 键 :" +
            Bytes.toString(CellUtil.cloneRow(cell)));
    //得到列族
        System.out.println(" 列 族 " +
            Bytes.toString(CellUtil.cloneFamily(cell)));
        System.out.println(" 列 :" +
            Bytes.toString(CellUtil.cloneQualifier(cell)));
        System.out.println(" 值 :" +
            Bytes.toString(CellUtil.cloneValue(cell)));
      }
    }
  }

  public static void deleteMultiRow(String tableName, String... rows)
      throws IOException{
    Table table = connection.getTable(TableName.valueOf(tableName));
    List<Delete> deleteList = new ArrayList<Delete>();
    for(String row : rows){
      Delete delete = new Delete(Bytes.toBytes(row));
//      //删除最新版本
//      delete.addColumn();
//      //删除所有版本
//      delete.addColumns();
      deleteList.add(delete);
    }
    table.delete(deleteList);
    table.close();
  }

  public static void main(String[] args) throws IOException {
//    System.out.println(isTableExist("stu"));
//    createNameSpace("demo");
//    createTable("demo:stu", "info1", "info2");
//    dropTable("stu");
//    System.out.println(isTableExist("stu"));
//    addRowData("demo:stu","1001","info1","sex","male");
//    getRow("demo:stu","1001");
//    getRowQualifier("demo:stu","1001","info1","add");
    getAllRows("demo:stu");
    close();
  }

}
