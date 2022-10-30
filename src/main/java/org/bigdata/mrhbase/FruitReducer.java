package org.bigdata.mrhbase;

import java.io.IOException;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Xu Qi
 * @since 2022/10/29
 */
public class FruitReducer extends TableReducer<ImmutableBytesWritable, Put, NullWritable> {

  @Override
  protected void reduce(ImmutableBytesWritable key, Iterable<Put> values,
      Reducer<ImmutableBytesWritable, Put, NullWritable, Mutation>.Context context)
      throws IOException, InterruptedException {
    //读出来的每一行数据写入到 fruit_mr 表中
    for(Put put: values){
      context.write(NullWritable.get(), put);
    }
  }
}
