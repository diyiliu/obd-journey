package com.tiza.util;

import java.io.InputStream;
import java.sql.*;
import java.util.Properties;

/**
 * Description: JdbcUtil
 * Author: DIYILIU
 * Update: 2018-07-23 17:19
 */
public class JdbcUtil {
    private static String DRIVER = "com.mysql.jdbc";
    private static String URL = "jdbc:mysql://localhost:3306/test";
    private static String USERNAME = "root";
    private static String PASSWORD = "123456";

    static {
        //加载数据库配置
        loadConfig();
    }

    /**
     * 加载数据库配置信息，并给相关的属性赋值
     */
    public static void loadConfig() {
        try (InputStream in = ClassLoader.getSystemResourceAsStream("jdbc.properties")) {
            Properties prop = new Properties();
            prop.load(in);
            USERNAME = prop.getProperty("jdbc.username");
            PASSWORD = prop.getProperty("jdbc.password");
            DRIVER = prop.getProperty("jdbc.driver");
            URL = prop.getProperty("jdbc.url");
        } catch (Exception e) {
            throw new RuntimeException("读取数据库配置文件异常！", e);
        }
    }


    /**
     * 连接数据库
     *
     * @return 链接数据库对象
     */
    public Connection getConnection() {
        Connection conn = null;
        try {
            Class.forName(DRIVER);
            conn = DriverManager.getConnection(URL, USERNAME, PASSWORD);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return conn;
    }

    /**
     * 释放相应的资源
     *
     * @param rs
     * @param pstmt
     * @param conn
     */
    public void closeAll(ResultSet rs, PreparedStatement pstmt, Connection conn) {
        try {
            if (rs != null) {
                rs.close();
            }
            if (pstmt != null) {
                pstmt.close();
            }
            if (conn != null) {
                conn.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    public void execute(String sql){
        Connection connection = null;
        PreparedStatement statement = null;
        try {
            connection = getConnection();
            statement = connection.prepareStatement(sql);
            statement.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            try {
                if (statement != null) {
                    statement.close();
                }
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

}
