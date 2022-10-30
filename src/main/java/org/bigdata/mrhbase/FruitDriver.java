package org.bigdata.mrhbase;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author Xu Qi
 * @since 2022/10/29
 */
public class FruitDriver implements Tool {

  private Configuration configuration;


  @Override
  public int run(String[] strings) throws Exception {
    //得到 Configuration
    Configuration conf = this.getConf();
    //创建 Job 任务
    Job job = Job.getInstance(conf,
        this.getClass().getSimpleName());
    job.setJarByClass(FruitDriver.class);
    //配置 Job
    Scan scan = new Scan();
    scan.setCacheBlocks(false);
    scan.setCaching(500);
    //设置 Mapper，注意导入的是 mapreduce 包下的，不是 mapred 包下的，后者是老版本
    TableMapReduceUtil.initTableMapperJob(
        "fruit", //数据源的表名
        scan, //scan 扫描控制器
        FruitMapper.class,//设置 Mapper 类
        ImmutableBytesWritable.class,//设置 Mapper 输出 key 类型
        Put.class,//设置 Mapper 输出 value 值类型
        job//设置给哪个 JOB
    );
    //设置 Reducer
    TableMapReduceUtil.initTableReducerJob("fruit1",
        FruitReducer.class, job);
    //设置 Reduce 数量，最少 1 个
    job.setNumReduceTasks(1);
    boolean isSuccess = job.waitForCompletion(true);
    if (!isSuccess) {
      throw new IOException("Job running with error");
    }
    return isSuccess ? 0 : 1;
  }

  @Override
  public void setConf(org.apache.hadoop.conf.Configuration conf) {
    configuration = conf;

  }

  @Override
  public org.apache.hadoop.conf.Configuration getConf() {
    return configuration;
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum", "localhost");
    conf.set("hbase.zookeeper.property.clientPort", "2181");
    int status = ToolRunner.run(conf, new FruitDriver(), args);
    System.exit(status);
  }
}
