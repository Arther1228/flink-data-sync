package com.suncreate.bigdata.flink.sync.utils;

import lombok.extern.log4j.Log4j2;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * @author yangliangchuang 2023/6/26 14:54
 */
@Log4j2
public class DateUtil {

    public static final String DATETIME_PATTERN = "yyyy-MM-dd HH:mm:ss";

    public static final String UNSIGNED_DATETIME_PATTERN = "yyyyMMddHHmmss";

    private static final String[] DATE_FORMATS = {
            DATETIME_PATTERN,
            UNSIGNED_DATETIME_PATTERN
            // 添加其他可能的日期格式
    };

    /**
     * 是否在时间范围内
     *
     * @param startDate
     * @param endDate
     * @param inputTime
     * @return
     */
    public static boolean suitableTime(LocalDate startDate, LocalDate endDate, String inputTime) {
        boolean result = false;
        try {
            LocalDate inputDate = parseDate(inputTime);
            if (inputDate == null) {
                return false;
            }
            result = inputDate.isAfter(startDate) && inputDate.isBefore(endDate);
        } catch (Exception e) {
            log.error("判断时间是否在区间范围内出现异常，{}", e.getMessage());
        }
        return result;
    }

    /**
     * 解析日期
     *
     * @param inputTime
     * @return
     */
    public static LocalDate parseDate(String inputTime) {
        LocalDate inputDate;
        for (String format : DATE_FORMATS) {
            try {
                inputDate = LocalDate.parse(inputTime, DateTimeFormatter.ofPattern(format));
                return inputDate;
            } catch (Exception e) {
                log.warn("解析日期失败：{}", e.getMessage());
            }
        }
        return null;
    }

    /**
     * 生成当前时间
     *
     * @return
     */
    public static String nowDate() {
        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter outputFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
        String nowDate = now.format(outputFormatter);
        return nowDate;
    }
}
