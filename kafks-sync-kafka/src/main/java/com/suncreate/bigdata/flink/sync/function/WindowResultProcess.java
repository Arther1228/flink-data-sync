package com.suncreate.bigdata.flink.sync.function;

import com.suncreate.bigdata.flink.sync.model.VehicleDataSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.stream.Collectors;

/**
 *
 */
public class WindowResultProcess extends ProcessWindowFunction<VehicleDataSource, Object, Tuple, TimeWindow> {
    @Override
    public void process(Tuple tuple, Context context, Iterable<VehicleDataSource> elements, Collector<Object> out) throws Exception {
        String dateTime = ((Tuple1<String>) tuple).f0;

        Queue<VehicleDataSource> queue = new PriorityQueue<>(3,
                (c1, c2) -> c1.getTotalCount() >= c2.getTotalCount() ? 1 : -1);

        long totalCount = 0L;

        for (VehicleDataSource element : elements) {
            long count = element.getTotalCount();//某个分类的总销售额
            totalCount += count;
            if (queue.size() < 3) {//小顶堆size<3,说明数不够,直接放入
                queue.add(element);
            } else {//小顶堆size=3,说明,小顶堆满了,进来一个需要比较
                //"取出"顶上的(不是移除)
                VehicleDataSource top = queue.peek();
                if (element.getTotalCount() > top.getTotalCount()) {
                    //queue.remove(top);//移除指定的元素
                    queue.poll();//移除顶上的元素
                    queue.add(element);
                }
            }
        }

        List<String> top3Result = queue.stream()
                .sorted((c1, c2) -> c1.getTotalCount() > c2.getTotalCount() ? -1 : 1)
                .map(c -> "(分类：" + c.getDataSource3() + " 分类总数：" + c.getTotalCount() + ")")
                .collect(Collectors.toList());
        System.out.println("时间 ： " + dateTime + "  总数 : " + totalCount + " top3:\n" + StringUtils.join(top3Result, ",\n"));
        System.out.println("-------------");
    }
}