package com.lym.mr1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author:李雁敏
 * @create:2019-09-10 15:57
 */
public class FruitReducer extends TableReducer<LongWritable, Text, NullWritable> {
//    String cf1 = null;
//
//    @Override
//    protected void setup(Context context) throws IOException, InterruptedException {
//        Configuration configuration = context.getConfiguration();
//        cf1 = configuration.get("cf1");
//    }

    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //遍历values
        for (Text value : values) {
            //获取每一行数据
            String[] split = value.toString().split("\t");
            //创建Put对象
            Put put = new Put(Bytes.toBytes(split[0]));
            //给put对象赋值
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(split[1]));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("color"), Bytes.toBytes(split[2]));
            //写出
            context.write(NullWritable.get(), put);
        }
    }
}
