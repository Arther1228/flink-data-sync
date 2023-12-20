package com.suncreate.bigdata.flink.sync.util;

import lombok.extern.log4j.Log4j2;

import java.sql.*;

/**
 * @author yangliangchuang 2023/3/21 11:01
 */
@Log4j2
public class JdbcSqlUtil {

    public static String createInsertSQL(String driverName, String url, String user, String password, String tableName) throws ClassNotFoundException {
        Class.forName(driverName);

        String result = "";
        try (Connection conn = DriverManager.getConnection(url, user, password);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName);) {
            ResultSetMetaData meta = rs.getMetaData();
            int columnCount = meta.getColumnCount();

            StringBuilder sb = new StringBuilder();
            sb.append("INSERT INTO ").append(tableName).append(" (");
            for (int i = 1; i <= columnCount; i++) {
                sb.append(meta.getColumnName(i));
                if (i < columnCount) {
                    sb.append(", ");
                }
            }
            sb.append(") VALUES (");

            for (int i = 1; i <= columnCount; i++) {
                sb.append("?");
                if (i < columnCount) {
                    sb.append(", ");
                }
            }
            sb.append(")");

            result = sb.toString();
            log.debug("insert sql: " + result);
        } catch (SQLException e) {
            log.error("error: " + e.getMessage());
        }
        return result;
    }


}
