package com.suncreate.bigdata.flink.sync.function;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.log4j.Log4j2;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;

/**
 * 一段时间没有数据，提示可以关闭程序
 *
 * @author HP
 */
@Log4j2
public class PromptFunction extends KeyedProcessFunction<Object, JSONObject, JSONObject> {

    /**
     * 超过十分钟没有合适的数据，认为是同步完成
     */
    private final static long TIME_OUT_MILLS = 60 * 1000;

    private transient ValueState<Boolean> hasDataState;

    @Override
    public void open(Configuration parameters) {
        // 初始化状态，用于标记是否有数据
        ValueStateDescriptor<Boolean> hasDataDescriptor = new ValueStateDescriptor<>("hasDataState", Boolean.class);
        hasDataState = getRuntimeContext().getState(hasDataDescriptor);
    }


    @Override
    public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws IOException {
        // 收到数据时，更新状态为有数据，并注册一个定时器
        hasDataState.update(true);
        ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + TIME_OUT_MILLS);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<JSONObject> out) throws IOException {

        // 定时器触发时，检查状态是否有数据，如果没有则发出提示
        if (!hasDataState.value()) {
            log.warn("已经十分钟没有新数据了，可以认为同步完成");
        }
        // 无论是否有数据，定时器触发后都重置状态
        hasDataState.clear();
    }

}
