package com.lym.utils;

import com.lym.constants.Constants;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * @author:李雁敏
 * @create:2019-09-12 11:26
 * 创建命名空间
 * 判断表是否存在
 * 创建表（三张表）
 */
public class HBaseUtils {

    //创建命名空间
    public static void createNameSpace(String nameSpace) throws IOException {
        //获取Connection对象
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
        //获取Admin对象
        Admin admin = connection.getAdmin();
        //构建命名空间描述器
        NamespaceDescriptor build = NamespaceDescriptor.create(nameSpace).build();
        //创建命名空间
        admin.createNamespace(build);
        //关闭资源
        admin.close();
        connection.close();
    }

    //判断表是否存在
    private static boolean isTableExist(String tableName) throws IOException {
        //获取Connection对象
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
        //获取Admin对象
        Admin admin = connection.getAdmin();
        //判断表是否存在
        boolean exists = admin.tableExists(TableName.valueOf(tableName));
        //关闭资源
        admin.close();
        connection.close();
        return exists;
    }

    //创建表
    public static void createTable(String tableName, int versions, String... cfs) throws IOException {
        //判断是否传入了列簇信息
        if (cfs.length <= 0) {
            System.out.println("请设置列簇信息！！！");
            return;
        }
        //判断吧是否存在
        if (isTableExist(tableName)) {
            System.out.println(tableName + "表以存在");
            return;
        }
        //获取Connection对象
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
        //获取Admin对象
        Admin admin = connection.getAdmin();
        //创建表描述器
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        //循坏添加列簇信息
        for (String cf : cfs) {
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
            //设置版本
            hColumnDescriptor.setMaxVersions(versions);
            hTableDescriptor.addFamily(hColumnDescriptor);
        }
        //创建表操作
        admin.createTable(hTableDescriptor);
        //关闭资源
        admin.close();
        connection.close();
    }
}
