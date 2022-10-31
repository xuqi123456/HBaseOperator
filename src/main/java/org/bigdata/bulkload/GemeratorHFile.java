package org.bigdata.bulkload;

import java.io.IOException;
import javax.naming.Context;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author Xu Qi
 * @since 2022/10/31
 */
public class GemeratorHFile {

  public static class HFileMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

    @Override
    /*输入key的类型 输入value的类型 输出key的类型 输出value的类型*/
    protected void map(LongWritable key, Text value,
        Context context) throws IOException, InterruptedException {

      String[] lineWords = value.toString().split("\t");
      String rowKey = lineWords[0];
      ImmutableBytesWritable row = new ImmutableBytesWritable(Bytes.toBytes(rowKey));

      Put put = new Put(Bytes.toBytes(rowKey));
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(lineWords[1]));
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("color"), Bytes.toBytes(lineWords[2]));
      context.write(row, put);
    }
  }

  public static void bulkLoad(Configuration conf, String inPath, String outPath, String tableName)
      throws IOException, InterruptedException, ClassNotFoundException {

    //封装Job
    Job job = Job.getInstance(conf, "Batch Import HBase Table：" + tableName);
    job.setJarByClass(GemeratorHFile.class);

    //指定输入路径
    FileInputFormat.setInputPaths(job, new Path(inPath));

    //指定输出路径[如果输出路径存在，就将其删除]
    Path output = new Path(outPath);
    FileSystem fs = output.getFileSystem(conf);
    if (fs.exists(output)) {
      fs.delete(output, true);
    }
    FileOutputFormat.setOutputPath(job, new Path(outPath));

    //指定map相关的代码
    job.setMapperClass(HFileMapper.class);
    job.setMapOutputKeyClass(ImmutableBytesWritable.class);
    job.setMapOutputValueClass(Put.class);

    //禁用Reduce
    job.setNumReduceTasks(0);

    Connection connection = ConnectionFactory.createConnection(conf);
    TableName outTableName = TableName.valueOf(tableName);
    HFileOutputFormat2.configureIncrementalLoad(job, connection.getTable(outTableName),
        connection.getRegionLocator(outTableName));
    boolean isSuccess = job.waitForCompletion(true);
    if (!isSuccess) {
      throw new IOException("Job running with error");
    }
  }

  public static void main(String[] args) throws Exception {
    String input = "hdfs://localhost:9000/fruit.tsv";
    String output = "hdfs://localhost:9000/csv/";
    String tableName = "bulkTest";
    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum", "localhost");
    conf.set("hbase.zookeeper.property.clientPort", "2181");
    bulkLoad(conf, input, output, tableName);
    Connection conn = ConnectionFactory.createConnection(conf);
    Admin admin = conn.getAdmin();
    Table table = conn.getTable(TableName.valueOf(tableName));
    RegionLocator locator = conn.getRegionLocator(
        TableName.valueOf(tableName));
    LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
    loader.doBulkLoad(new Path(output), admin, table, locator);
  }
}
