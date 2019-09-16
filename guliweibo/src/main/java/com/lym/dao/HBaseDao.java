package com.lym.dao;

import com.lym.constants.Constants;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
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

    //取关
    public static void deleteAttends(String uid, String... dels) throws IOException {
        //获取connection对象
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
        //操作用户关系表
        //获取用户关系表对象
        Table relaTable = connection.getTable(TableName.valueOf(Constants.RELATION_TABLE));
        //创建一个集合，用于存放用户关系表的delete对象
        ArrayList<Delete> relaDeletes = new ArrayList<Delete>();
        //创建操作者的delete对象
        Delete uidDelete = new Delete(Bytes.toBytes(uid));
        //循环创建被取关者的delete对象
        for (String del : dels) {
            //给操作者的delete对象赋值
            uidDelete.addColumns(Bytes.toBytes(Constants.RELATION_TABLE_CF1), Bytes.toBytes(del));
            //创建被取关者的delete对象
            Delete delDelete = new Delete(Bytes.toBytes(del));
            //被取关者的delete对象赋值
            delDelete.addColumns(Bytes.toBytes(Constants.RELATION_TABLE_CF2), Bytes.toBytes(uid));
            //将被取关者的delete对象添加至集合
            relaDeletes.add(delDelete);
        }
        //将操作者的delete对象添加至集合
        relaDeletes.add(uidDelete);
        //执行用户关系表的删除操作
        relaTable.delete(relaDeletes);
        //操作用户收件箱表
        //获取收件箱表对象
        Table inboxTable = connection.getTable(TableName.valueOf(Constants.INBOX_TABLE));
        //创建操作者的delete对象
        Delete inboxDelete = new Delete(Bytes.toBytes(uid));
        //给操作者的delete对象赋值
        for (String del : dels) {
            inboxDelete.addColumns(Bytes.toBytes(Constants.INBOX_TABLE_CF), Bytes.toBytes(del));
        }
        //执行收件箱表的删除操作
        inboxTable.delete(inboxDelete);
        //关闭资源
        relaTable.close();
        inboxTable.close();
        connection.close();
    }

    //获取用户的初始化页面
    public static void getInit(String uid) throws IOException {
        //获取connection对象
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
        //获取收件箱表对象
        Table inboxTable = connection.getTable(TableName.valueOf(Constants.INBOX_TABLE));
        //获取微博内容表对象
        Table contTable = connection.getTable(TableName.valueOf(Constants.CONTENT_TABLE));
        //创建收件箱表Get对象，并获取数据(设置最大版本)
        Get inboxGet = new Get(Bytes.toBytes(uid));
        inboxGet.setMaxVersions();
        Result result = inboxTable.get(inboxGet);
        //遍历获取对象
        for (Cell cell : result.rawCells()) {
            //构建微博内容表Get对象
            Get contGet = new Get(CellUtil.cloneValue(cell));
            //获取该Get对象的数据内容
            Result contResult = contTable.get(contGet);
            //解析内容并打印
            for (Cell contCell : contResult.rawCells()) {
                System.out.println("RK:" + Bytes.toString(CellUtil.cloneRow(contCell)) +
                        ",CF" + Bytes.toString(CellUtil.cloneFamily(contCell)) +
                        ",CN" + Bytes.toString(CellUtil.cloneQualifier(contCell)) +
                        ",Value" + Bytes.toString(CellUtil.cloneValue(contCell)));
            }
        }
        //关闭资源
        inboxTable.close();
        contTable.close();
        connection.close();
    }

    //获取某个人的所有微博详情
    public static void getWeiBo(String uid) throws IOException {
        //获取connection对象
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
        //获取内容表对象
        Table contTable = connection.getTable(TableName.valueOf(Constants.CONTENT_TABLE));
        //构建Scan对象
        Scan scan = new Scan();
        //构建过滤器
        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(uid + "_"));
        scan.setFilter(rowFilter);
        //获取数据
        ResultScanner scanner = contTable.getScanner(scan);
        //解析数据并打印
        for (Result result : scanner) {
            for (Cell cell : result.rawCells()) {
                System.out.println("RK:" + Bytes.toString(CellUtil.cloneRow(cell)) +
                        ",CF" + Bytes.toString(CellUtil.cloneFamily(cell)) +
                        ",CN" + Bytes.toString(CellUtil.cloneQualifier(cell)) +
                        ",Value" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
        //关闭资源
        contTable.close();
        connection.close();
    }
}
