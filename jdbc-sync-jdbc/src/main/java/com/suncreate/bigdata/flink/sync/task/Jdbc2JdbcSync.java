package com.suncreate.bigdata.flink.sync.task;

import com.suncreate.bigdata.flink.sync.util.JdbcSqlUtil;
import com.suncreate.bigdata.flink.sync.util.PropertiesFactory;
import com.suncreate.bigdata.flink.sync.util.RowTypeInfoUtil;
import lombok.extern.log4j.Log4j2;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

import java.util.LinkedHashMap;
import java.util.Properties;

/**
 * @author admin
 * @desc JDBCInputFormat DataSet JDBCOutputFormat 完成数据同步
 */
@Log4j2
public class Jdbc2JdbcSync {

    private static Properties properties = PropertiesFactory.getProperties();

    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment streamEnv = ExecutionEnvironment.getExecutionEnvironment();

        String inputUserName = properties.getProperty("input.username");
        String inputPassWord = properties.getProperty("input.password");
        String inputDriverName = properties.getProperty("input.drivername");
        String inputDbURL = properties.getProperty("input.dbURL");
        String inputTableName = properties.getProperty("input.tablename");
        int fetchSize = Integer.valueOf(properties.getProperty("fetchSize"));

        //得到字段列表及类型
        LinkedHashMap<String, Class<?>> fieldsNameMap = RowTypeInfoUtil.getFieldsName(inputDriverName, inputDbURL, inputUserName, inputPassWord, inputTableName);
        RowTypeInfo rowTypeInfoList = RowTypeInfoUtil.getRowTypeInfoByfieldMap(fieldsNameMap);

        // 配置连接信息
        JDBCInputFormat jdbcInput = JDBCInputFormat.buildJDBCInputFormat()
                .setDrivername(inputDriverName)
                .setDBUrl(inputDbURL)
                .setUsername(inputUserName)
                .setPassword(inputPassWord)
                .setQuery("select * from " + inputTableName)
                .setFetchSize(fetchSize)
                .setRowTypeInfo(rowTypeInfoList)
                .finish();

        // 读取数据
        DataSet<Row> dataSet = streamEnv.createInput(jdbcInput);

        String outputUserName = properties.getProperty("output.username");
        String outputPassWord = properties.getProperty("output.password");
        String outputDriverName = properties.getProperty("output.drivername");
        String outputDbURL = properties.getProperty("output.dbURL");
        String outputTableName = properties.getProperty("output.tablename");

        // 设置插入语句
        String insertSQL = JdbcSqlUtil.createInsertSQL(outputDriverName, outputDbURL, outputUserName, outputPassWord, outputTableName);
        JDBCOutputFormat jdbcOutput = JDBCOutputFormat.buildJDBCOutputFormat()
                .setDrivername(outputDriverName)
                .setDBUrl(outputDbURL)
                .setUsername(outputUserName)
                .setPassword(outputPassWord)
                .setQuery(insertSQL)
                .finish();

        dataSet.output(jdbcOutput);

        streamEnv.execute("JDBC Data Integration");
    }
}
