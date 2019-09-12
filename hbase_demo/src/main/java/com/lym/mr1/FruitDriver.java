package com.lym.mr1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author:李雁敏
 * @create:2019-09-10 15:57
 */
public class FruitDriver implements Tool {
    public static void main(String[] args) {
        try {
            Configuration configuration = new Configuration();
            int run = ToolRunner.run(configuration, new FruitDriver(), args);
            System.exit(run);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //定义一个Configuration
    private Configuration configuration = null;

    public int run(String[] args) throws Exception {
        //获取job对象
        Job job = Job.getInstance(configuration);
        //设置类路劲
        job.setJarByClass(FruitDriver.class);
        //设置mapper和Mapper输出的KV类型
        job.setMapperClass(FruitMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        //设置reducer类
        TableMapReduceUtil.initTableReducerJob(args[1], FruitReducer.class, job);
        //设置输入参数
        FileInputFormat.setInputPaths(job, new Path(args[0]));
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


}
