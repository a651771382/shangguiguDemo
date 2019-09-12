package com.lym.dao;

import com.lym.constants.Constants;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;

/**
 * @author:李雁敏
 * @create:2019-09-12 11:47
 * 发布微博
 * 删除微博
 * 关注用户
 * 取关用户
 * 获取用户微博详情
 * 获取某个用户的初始化页面
 */
public class HBaseDao {
    //发布微博
    public static void publishWeibo(String uid, String content) throws IOException {
        //获取Connection对象
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
        //第一部分操作微博内容表
        //获取微博内容表对象
        Table contTable = connection.getTable(TableName.valueOf(Constants.CONTENT_TABLE));
        //获取当前时间戳
        long ts = System.currentTimeMillis();
        //获取rowkey
        String rowKey = uid + "_" + ts;
        //创建put对象
        Put contPut = new Put(Bytes.toBytes(rowKey));
        //给put对象赋值
        contPut.addColumn(Bytes.toBytes(Constants.CONTENT_TABLE_CF), Bytes.toBytes("content"), Bytes.toBytes(content));
        //执行插入数据操作
        contTable.put(contPut);
        //操作微博收件箱表
        //获取用户关系表对象
        Table relatTable = connection.getTable(TableName.valueOf(Constants.RELATION_TABLE));
        //获取当前发布微博人的fans列簇的数据
        Get get = new Get(Bytes.toBytes(uid));
        get.addFamily(Bytes.toBytes(Constants.RELATION_TABLE_CF2));
        Result result = relatTable.get(get);
        //创建一个集合，用于存放微博内容表的Put对象
        ArrayList<Put> inboxPuts = new ArrayList<Put>();
        for (Cell cell : result.rawCells()) {
            //构建微博收件箱表的Put对象
            Put inboxPut = new Put(CellUtil.cloneQualifier(cell));
            //给微博收件箱表put对象赋值
            inboxPut.addColumn(Bytes.toBytes(Constants.INBOX_TABLE_CF), Bytes.toBytes(uid), Bytes.toBytes(rowKey));
            //将微博收件箱的put对象存入集合
            inboxPuts.add(inboxPut);
        }
        //判断是否有粉丝
        if (inboxPuts.size() > 0) {
            //获取收件箱对象
            Table inboxTable = connection.getTable(TableName.valueOf(Constants.INBOX_TABLE));
            //执行收件箱表数据插入操作
            inboxTable.put(inboxPuts);
            //关闭收件箱资源
            inboxTable.close();
        }
        //关闭资源
        relatTable.close();
        contTable.close();
        connection.close();
    }

    //关注用户
    public static void addAttends(String uid, String... attends) throws IOException {
        //检验是否添加了待关注的人
        if (attends.length <= 0) {
            System.out.println("请选择待关注的人");
            return;
        }
        //获取Connection对象
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
        //第一部分操作用户关系表
        //获取用户关系表对象
        Table relaTable = connection.getTable(TableName.valueOf(Constants.RELATION_TABLE));
        //创建一个集合，用于存放用户关系表的put对象
        ArrayList<Put> relaPuts = new ArrayList<Put>();
        //创建操作者的put对象
        Put uidPut = new Put(Bytes.toBytes(uid));
        //循坏创建被关注人的put对象
        for (String attend : attends) {
            //给操作者的put对象赋值
            uidPut.addColumn(Bytes.toBytes(Constants.RELATION_TABLE_CF1), Bytes.toBytes(attend), Bytes.toBytes(attend));
            //创建被关注者的put对象
            Put attendPut = new Put(Bytes.toBytes(attend));
            //给被关注者的put对象赋值
            attendPut.addColumn(Bytes.toBytes(Constants.RELATION_TABLE_CF2), Bytes.toBytes(uid), Bytes.toBytes(uid));
            //将被关注者的put对象放入集合
            relaPuts.add(attendPut);
        }
        //将操作者的put对象添加至集合
        relaPuts.add(uidPut);
        //执行用户关系表插入数据操作
        relaTable.put(relaPuts);
        //第二部分操作收件箱表
        //获取微博内容表对象
        Table connTable = connection.getTable(TableName.valueOf(Constants.CONTENT_TABLE));
        //创建收件箱表的put对象
        Put inboxPut = new Put(Bytes.toBytes(uid));
        //循坏attends,获取每个被关注者的近期发布的微博
        for (String attend : attends) {
            //获取当前呗关注者近期发布的微博
            Scan scan = new Scan(Bytes.toBytes(attend + "_"), Bytes.toBytes(attend + "|"));
            ResultScanner resultScanner = connTable.getScanner(scan);
            //定义一个时间戳
            long ts = System.currentTimeMillis();
            //对获取的值进行遍历
            for (Result result : resultScanner) {
                //给收件箱表的put对象赋值
                inboxPut.addColumn(Bytes.toBytes(Constants.INBOX_TABLE_CF), Bytes.toBytes(attend), ts++, result.getRow());
            }
        }

        //判断当前的put对象是否为空
        if (!inboxPut.isEmpty()) {
            //获取收件箱表对象
            Table inboxTable = connection.getTable(TableName.valueOf(Constants.INBOX_TABLE));
            //插入数据
            inboxTable.put(inboxPut);
            //关闭收件箱表对象
            inboxTable.close();
        }
        //关闭资源
        relaTable.close();
        connTable.close();
        connection.close();
    }
}
