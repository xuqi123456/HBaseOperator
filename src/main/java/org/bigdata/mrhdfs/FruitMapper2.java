package org.bigdata.mrhdfs;

import java.io.IOException;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Xu Qi
 * @since 2022/10/30
 */
public class FruitMapper2 extends Mapper<LongWritable,
    Text, ImmutableBytesWritable, Put> {

  @Override
  protected void map(LongWritable key, Text value, Context
      context) throws IOException, InterruptedException {
  //从 HDFS 中读取的数据
    String lineValue = value.toString();
  //读取出来的每行数据使用\t 进行分割，存于 String 数组
    String[] values = lineValue.split("\t");
  //根据数据中值的含义取值
    String rowKey = values[0];
    String name = values[1];
    String color = values[2];
  //初始化 rowKey
    ImmutableBytesWritable rowKeyWritable = new
        ImmutableBytesWritable(Bytes.toBytes(rowKey));
  //初始化 put 对象
    Put put = new Put(Bytes.toBytes(rowKey));
  //参数分别:列族、列、值
    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"),
        Bytes.toBytes(name));
    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("color"),
        Bytes.toBytes(color));
    context.write(rowKeyWritable, put);
  }
}
