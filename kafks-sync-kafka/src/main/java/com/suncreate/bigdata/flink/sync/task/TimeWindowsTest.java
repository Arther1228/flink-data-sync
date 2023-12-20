package com.suncreate.bigdata.flink.sync.task;

import com.suncreate.bigdata.flink.sync.function.DataSourceAggregate;
import com.suncreate.bigdata.flink.sync.function.WindowResult;
import com.suncreate.bigdata.flink.sync.function.WindowResultProcess;
import com.suncreate.bigdata.flink.sync.model.VehicleDataSource;
import com.suncreate.bigdata.flink.sync.source.MySource;
import lombok.extern.log4j.Log4j2;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;

/**
 * @author yangliangchuang
 * @desc kafka集群数据同步程序
 * @desc flink 1.7.2 TumblingProcessingTimeWindows 如果这样用，会报错
 */
@Log4j2
public class TimeWindowsTest {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.AT_LEAST_ONCE);
        env.setParallelism(1);
        //设置checkpoint保存大型State一致性快照的存储位置
//        env.setStateBackend((StateBackend) new FsStateBackend(properties.getProperty("flink.checkpoint")));

        // 测试数据
        SingleOutputStreamOperator<Tuple2<String, Long>> originDataStream = env.addSource(new MySource()).setParallelism(1).name("origin kafka data");

        // 3.数据统计，先按照data_source3字段统计，再统计总数
        SingleOutputStreamOperator<VehicleDataSource> tempAggResult = originDataStream
                .keyBy(0)
                .window(TumblingProcessingTimeWindows.of(Time.days(1), Time.hours(-8)))
                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(1)))
                .aggregate(new DataSourceAggregate(), new WindowResult());

        tempAggResult.print("初步聚合结果");

        tempAggResult.keyBy("dateTime")
                .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
                .process(new WindowResultProcess());

        env.execute("kafka-sync-kafka");
    }
}
