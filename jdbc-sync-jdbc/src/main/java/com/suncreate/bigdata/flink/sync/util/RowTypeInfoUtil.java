package com.suncreate.bigdata.flink.sync.util;

import lombok.extern.log4j.Log4j2;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.LinkedHashMap;

/**
 * @author yangliangchuang 2023/3/21 10:33
 */
@Log4j2
public class RowTypeInfoUtil {

    /**
     * 根据表名获取【字段名->类型】 map
     *
     * @param url
     * @param user
     * @param password
     * @param tableName
     * @return
     */
    public static LinkedHashMap<String, Class<?>> getFieldsName(String driverName, String url, String user, String password, String tableName) throws ClassNotFoundException {
        Class.forName(driverName);

        // 遍历结果集，构建字段名称和类型的映射关系
        LinkedHashMap<String, Class<?>> fields = new LinkedHashMap<>();
        ResultSet rs = null;
        // 获取数据库连接
        try (Connection conn = DriverManager.getConnection(url, user, password)) {
            DatabaseMetaData metaData = conn.getMetaData();
            rs = metaData.getColumns(null, null, tableName, null);
            while (rs.next()) {
                String fieldName = rs.getString("COLUMN_NAME");
                int fieldType = rs.getInt("DATA_TYPE");
                Class<?> fieldClass = JDBCTypeToJavaClassConverter.convert(fieldType);
                fields.put(fieldName, fieldClass);
            }
            rs.close();
        } catch (Exception e) {
            log.error("获取fieldsName时发生错误：{}", e.getMessage());
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (Exception e) {
                    log.error("关闭 ResultSet时发生错误：{}", e.getMessage());
                }
            }
            return fields;
        }
    }

    /**
     * 根据fieldsMap 构建 RowTypeInfo
     *
     * @param fieldsMap
     * @return
     */
    public static RowTypeInfo getRowTypeInfoByfieldMap(LinkedHashMap<String, Class<?>> fieldsMap) {
        // 构造字段类型数组
        TypeInformation<?>[] fieldTypes = new TypeInformation<?>[fieldsMap.size()];

        int i = 0;
        for (HashMap.Entry<String, Class<?>> entry : fieldsMap.entrySet()) {
            fieldTypes[i] = TypeInformation.of(entry.getValue());
            i++;
        }
        // 创建RowTypeInfo对象
        RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes);
        return rowTypeInfo;
    }

}
