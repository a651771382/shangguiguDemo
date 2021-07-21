package com.atgui;

import java.sql.*;

public class KylinTest {
    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        //Kylin_JDBC驱动
        String KYLIN_DRIVER = "org.apache.kylin.jdbc.Driver";
        //kylin_URL
        String KYLIN_URL = "jdbc://kylin://192.168.159.131:7070/FirstProject";
        //Kylin的用户名
        String KYLIN_USER = "admin";
        //KYLIN的密码
        String KYLIN_PASSWD = "kylin";

        //注册驱动
        Class.forName(KYLIN_DRIVER);

        //获取JDBC连接
        Connection connection = DriverManager.getConnection(KYLIN_URL, KYLIN_USER, KYLIN_PASSWD);
        //预编译SQL
        PreparedStatement ps = connection.prepareStatement("SELECT SUM(sal) FROM emp GROUP BY deptno");

        //执行
        ResultSet resultSet = ps.executeQuery();
        while (resultSet.next()) {
            System.out.println(resultSet.getInt(1));
        }
        //关闭连接
        resultSet.close();
        ps.close();
        connection.close();
    }
}
