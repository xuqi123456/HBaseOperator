package org.bigdata.mrhbase;

import java.io.IOException;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Xu Qi
 * @since 2022/10/29
 */
public class FruitMapper extends TableMapper<ImmutableBytesWritable, Put> {

  @Override
  protected void map(ImmutableBytesWritable key, Result value,
      Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, Put>.Context context)
      throws IOException, InterruptedException {
    //将 fruit 的 name 和 color 提取出来，相当于将每一行数据读取出来放入到 Put对象中。
    Put put = new Put(key.get());
    //遍历添加 column 行
    for (Cell cell : value.rawCells()) {
      //添加/克隆列族:info
      if ("info".equals(Bytes.toString(CellUtil.cloneFamily(cell)))) {
        //添加/克隆列： name
        if ("name".equals(Bytes.toString(CellUtil.cloneQualifier(cell))
        )) {
          //将该列 cell 加入到 put 对象中
          put.add(cell);
          //添加/克隆列:color
        } else if ("color".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
          //向该列 cell 加入到 put 对象中
          put.add(cell);
        }
      }
    }
    //将从 fruit 读取到的每行数据写入到 context 中作为 map 的输出
    context.write(key, put);
  }
}

