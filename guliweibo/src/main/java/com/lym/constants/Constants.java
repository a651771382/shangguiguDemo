package com.lym.constants;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

/**
 * @author:李雁敏
 * @create:2019-09-12 11:31
 */
public class Constants {
    //HBase配置信息
    public static Configuration CONFIGURATION = HBaseConfiguration.create();
    //命名空间
    private static String NAMESPACE = "weibo";
    //微博内容表
    public static String CONTENT_TABLE = "weibo:content";
    //微博内容表列簇信息
    public static String CONTENT_TABLE_CF = "info";
    //微博内容表版本信息
    public static int CONTENT_TABLE_VERSIONS = 1;
    //用户关系表
    public static String RELATION_TABLE = "weibo:relation";
    //用户关系表列簇信息
    public static String RELATION_TABLE_CF1 = "attends";
    public static String RELATION_TABLE_CF2 = "fans";
    //用户关系表版本信息
    public static int RELATION_TABLE_VERSIONS = 1;
    //收件箱表
    public static String INBOX_TABLE = "weibo:inbox";
    //收件箱列簇信息
    public static String INBOX_TABLE_CF = "info";
    //收件箱表版本信息
    public static int INBOX_TABLE_VERSIONS = 2;
}
