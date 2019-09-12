package com.lym.hbase.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author:李雁敏
 * @create:2019-09-06 09:58
 * <p>
 * DDL:
 * 1.判断表是否存在
 * 2.创建表
 * 3.创建命名空间
 * 4.删除表
 * <p>
 * DML:
 * 1.插入数据
 * 2.查询数据 get（），scan（）
 * 3.删除数据
 */
public class TestAPI {
    private static Connection connection = null;
    private static Admin admin = null;

    static {
        try {
            //获取配置文件信息
            Configuration configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum", "ceshi,ceshi1,ceshi2");
            //创建连接对象
            connection = ConnectionFactory.createConnection(configuration);
            //创建Admin对象
            Connection connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //判断表是否存在
    public static boolean isTableExist(String tableName) throws IOException {
        //判断表是否存在
        boolean exists = admin.tableExists(TableName.valueOf(tableName));
        return exists;
    }

    //关闭资源
    public static void close() {
        if (admin != null) {
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    //创建表
    public static void createTable(String tableName, String... cfs) throws IOException {
        //判断是否存在列簇信息
        if (cfs.length <= 0) {
            System.out.println("请设置列簇信息");
            return;
        }

        //判断表是否存在
        if (isTableExist(tableName)) {
            System.out.println(tableName + "表已存在");
        }

        //创建表描述器
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        //循坏添加列簇信息
        for (String cf : cfs) {
            //创建列簇描述器
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
            //添加具体的列簇信息
            hTableDescriptor.addFamily(hColumnDescriptor);
        }
        //创建表
        admin.createTable(hTableDescriptor);
    }

    //删除表
    public static void dropTable(String tableName) throws IOException {
        //判断表是否存在
        if (!isTableExist(tableName)) {
            System.out.println(tableName + "表不存在！！");
        }
        //将表下线
        admin.disableTable(TableName.valueOf(tableName));
        //删除表
        admin.deleteTable(TableName.valueOf(tableName));
    }

    //创建命名空间
    public static void createNameSpace(String ns) {
        try {
            //创建命名空间描述器
            NamespaceDescriptor build = NamespaceDescriptor.create(ns).build();
            //创建命名空间
            admin.createNamespace(build);
        } catch (NamespaceExistException e) {
            System.out.println(ns + "命名空间已存在");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //向表里插入数据
    public static void putData(String tableName, String rowKey, String cf, String cn,
                               String value) throws IOException {
        //获取表的对象
        Table table = connection.getTable(TableName.valueOf(tableName));
        //创建put对象
        Put put = new Put(Bytes.toBytes(rowKey));
        //给put对象赋值
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn), Bytes.toBytes(value));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("sex"), Bytes.toBytes("male"));
        //插入数据
        table.put(put);
        //关闭连接
        table.close();
    }

    //获取数据
    public static void getData(String tableName, String rowkey, String cf, String cn) throws IOException {
        //获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));
        //创建get对象
        Get get = new Get(Bytes.toBytes(rowkey));
        //指定获取的列簇
//        get.addFamily(Bytes.toBytes(cf));
        //指定列簇和列名
        get.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn));
        //指定获取数据的版本数
        get.setMaxVersions();
        //获取数据
        Result result = table.get(get);
        //解析Result对象,并打印
        for (Cell cell : result.rawCells()) {
            //打印数据
            System.out.println("CF:" + Bytes.toString(CellUtil.cloneFamily(cell)) +
                    ",CN:" + Bytes.toString(CellUtil.cloneQualifier(cell)) + ",VALUE:" +
                    Bytes.toString(CellUtil.cloneValue(cell)));
        }
        //关闭表连接
        table.close();
    }

    //获取数据（scan）
    public static void sacnTable(String tableName) throws IOException {
        //获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));
        //创建一个scan对象
        Scan scan = new Scan(Bytes.toBytes("1001"), Bytes.toBytes("1003"));
        //扫描表
        ResultScanner scanner = table.getScanner(scan);
        //解析ResultScanner对象，并打印
        for (Result result : scanner) {
            for (Cell cell : result.rawCells()) {
                //打印数据
                System.out.println("Rowkey:" + Bytes.toString(CellUtil.cloneRow(cell)) + ",CF:" + Bytes.toString(CellUtil.cloneFamily(cell)) +
                        ",CN:" + Bytes.toString(CellUtil.cloneQualifier(cell)) + ",VALUE:" +
                        Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
        //关闭连接
        table.close();
    }

    //删除数据
    public static void deleteData(String tableName, String rowkey, String cf, String cn) throws IOException {
        //获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));
        //创建delete对象
        Delete delete = new Delete(Bytes.toBytes(rowkey));
        //设置删除的列
        delete.addColumns(Bytes.toBytes(cf), Bytes.toBytes(cn),1567647017606L);
        //执行删除操作
        table.delete(delete);
        //关闭连接
        table.close();
    }

    public static void main(String[] args) throws IOException {
        //测试表是否存在
//        System.out.println(isTableExist("stu5"));
        //创建表测试
//        createTable("stu5", "info1", "info2");
        //删除表测试
//        dropTable("stu5");
//        System.out.println(isTableExist("stu5"));
        //创建命名空间
//        createNameSpace("0408");
        //创建数据测试
//        putData("stu", "1002", "info2", "name", "zhangsan");
        //获取单行数据
//        getData("stu2", "1001", "info", "name");
        //测试扫描数据
//        sacnTable("stu");
        //测试删除
        deleteData("stu", "1009", "info1", "name");
        //关闭资源
        close();
    }
}
