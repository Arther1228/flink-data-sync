package com.suncreate.bigdata.flink.sync.util;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;

/**
 * @author admin
 */
public class JDBCTypeToJavaClassConverter {

    //人大金仓日期类型Type ID
    public static final int DATE_TIME_KB = 2001;

    public static Class<?> convert(int mysqlTypeId) {
        switch (mysqlTypeId) {
            case Types.BIT:
                return Boolean.class;
            case Types.TINYINT:
            case Types.INTEGER:
                return Integer.class;
            case Types.SMALLINT:
                return Short.class;
            case Types.BIGINT:
                return Long.class;
            case Types.FLOAT:
                return Float.class;
            case Types.DOUBLE:
                return Double.class;
            case Types.NUMERIC:
            case Types.DECIMAL:
                return BigDecimal.class;
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                return String.class;
            case Types.DATE:
                return Date.class;
            case Types.TIME:
                return Time.class;
            case Types.TIMESTAMP:
            case DATE_TIME_KB:
                return Timestamp.class;
            default:
                throw new IllegalArgumentException("Unsupported MySQL Type ID:" + mysqlTypeId);
        }
    }
}
