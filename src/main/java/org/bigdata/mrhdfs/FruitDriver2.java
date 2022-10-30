package org.bigdata.mrhdfs;


import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.bigdata.mrhbase.FruitDriver;

/**
 * @author Xu Qi
 * @since 2022/10/30
 */
public class FruitDriver2 implements Tool {
  private Configuration configuration = null;
  @Override
  public int run(String[] strings) throws Exception {
    //得到 Configuration
    Configuration conf = this.getConf();
    //创建 Job 任务
    Job job = Job.getInstance(conf, this.getClass().getSimpleName());
    job.setJarByClass(FruitDriver2.class);
    Path inPath = new
        Path("hdfs://localhost:9000/fruit.tsv");
    FileInputFormat.addInputPath(job, inPath);
    //设置 Mapper
    job.setMapperClass(FruitMapper2.class);
    job.setMapOutputKeyClass(ImmutableBytesWritable.class);
    job.setMapOutputValueClass(Put.class);
    //设置 Reducer
    TableMapReduceUtil.initTableReducerJob("fruit2",
        FruitReducer2.class, job);
    //设置 Reduce 数量，最少 1 个
    job.setNumReduceTasks(1);
    boolean isSuccess = job.waitForCompletion(true);
    if(!isSuccess){
      throw new IOException("Job running with error");
    }
    return 0;
  }

  @Override
  public void setConf(Configuration conf) {
    configuration = conf;
  }

  @Override
  public Configuration getConf() {
    return configuration;
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum", "localhost");
    conf.set("hbase.zookeeper.property.clientPort", "2181");
    int status = ToolRunner.run(conf, new FruitDriver2(), args);
    System.exit(status);
  }
}
