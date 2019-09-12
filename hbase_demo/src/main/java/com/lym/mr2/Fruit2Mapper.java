package com.lym.mr2;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author:李雁敏
 * @create:2019-09-10 16:46
 */
public class Fruit2Mapper extends TableMapper<ImmutableBytesWritable, Put> {
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        //创建Put对象
        Put put = new Put(key.get());

        //获取数据
        for (Cell cell : value.rawCells()) {
            //判断当前的cell是否为name列
            if ("name".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
                //给put对象赋值
                put.add(cell);
            }
        }
        //写出
        context.write(key, put);
    }
}
