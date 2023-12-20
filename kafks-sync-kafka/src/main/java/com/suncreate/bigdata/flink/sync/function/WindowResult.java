package com.suncreate.bigdata.flink.sync.function;

import com.suncreate.bigdata.flink.sync.model.VehicleDataSource;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;

/**
 * 自定义WindowFunction,实现如何收集窗口结果数据
 * interface WindowFunction<IN, OUT, KEY, W extends Window>
 * interface WindowFunction<Double, CategoryPojo, Tuple的真实类型就是String就是分类, W extends Window>
 */
public class WindowResult implements WindowFunction<Long, VehicleDataSource, Tuple, TimeWindow> {

    //定义一个时间格式化工具用来将当前时间(双十一那天订单的时间)转为String格式
    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @Override
    public void apply(Tuple tuple, TimeWindow window, Iterable<Long> input, Collector<VehicleDataSource> out) throws Exception {
        String category = ((Tuple1<String>) tuple).f0;

        Long count = input.iterator().next();

        long currentTimeMillis = System.currentTimeMillis();
        String dateTime = df.format(currentTimeMillis);

        VehicleDataSource categoryPojo = new VehicleDataSource(category, count, dateTime);
        out.collect(categoryPojo);
    }
}