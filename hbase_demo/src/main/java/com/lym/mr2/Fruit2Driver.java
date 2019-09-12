package com.lym.mr2;

import com.lym.mr1.FruitDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author:李雁敏
 * @create:2019-09-10 16:47
 */
public class Fruit2Driver implements Tool {
    private Configuration configuration = null;

    public int run(String[] strings) throws Exception {
        //获取job对象
        Job job = Job.getInstance(configuration);
        //设置主类路劲
        job.setJarByClass(Fruit2Driver.class);
        //设置Mapper输出KV类型
        TableMapReduceUtil.initTableMapperJob("fruit", new Scan(), Fruit2Mapper.class, ImmutableBytesWritable.class, Put.class, job);
        //设置Reducer输出的表
        TableMapReduceUtil.initTableReducerJob("fruit2", Fruit2Reducer.class, job);
        //提交任务
        boolean b = job.waitForCompletion(true);
        return b ? 0 : 1;
    }

    public void setConf(Configuration conf) {
        configuration = conf;
    }

    public Configuration getConf() {
        return configuration;
    }

    public static void main(String[] args) {
        try {
//            Configuration configuration = new Configuration();
            Configuration configuration = HBaseConfiguration.create();
            ToolRunner.run(configuration, new Fruit2Driver(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
