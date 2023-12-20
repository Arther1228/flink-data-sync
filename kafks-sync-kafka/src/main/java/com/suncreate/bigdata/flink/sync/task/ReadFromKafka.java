package com.suncreate.bigdata.flink.sync.task;

import com.alibaba.fastjson.JSONObject;
import com.suncreate.bigdata.flink.sync.model.KafkaMsgSchema;
import com.suncreate.bigdata.flink.sync.utils.PropertiesFactory;
import lombok.extern.log4j.Log4j2;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchemaWrapper;

import java.util.Properties;

/**
 * @author yangliangchuang
 * @desc kafka集群数据同步程序，为了使用雪亮集群资源，使用flink版本：1.7.2
 * @desc 数据统计功能暂未提供
 *
 */
@Log4j2
public class ReadFromKafka {

    private static Properties properties = PropertiesFactory.getProperties();

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.AT_LEAST_ONCE);
        env.setParallelism(1);
        //设置checkpoint保存大型State一致性快照的存储位置
        env.setStateBackend((StateBackend) new FsStateBackend(properties.getProperty("flink.checkpoint")));

        // 1.消费kafka
        String originTopicName = "motorVehicle";
        Properties props = new Properties();
        String bootstrap1 = properties.getProperty("origin.bootstrap.servers");
        props.put("bootstrap.servers", bootstrap1);
        props.put("group.id", properties.getProperty("group.id"));
        props.put("auto.offset.reset", properties.getProperty("auto.offset.reset"));

        FlinkKafkaConsumer011<JSONObject> kafkaConsumer = new FlinkKafkaConsumer011<>(
                originTopicName,
                new KeyedDeserializationSchemaWrapper<>(new KafkaMsgSchema()),
                props);

        // 从consumer 分组(在consumer中group.id的配置项)提交到Kafka broker的偏移位置开始读取分区。
        kafkaConsumer.setStartFromGroupOffsets();
        SingleOutputStreamOperator<JSONObject> originDataStream = env.addSource(kafkaConsumer).setParallelism(1).name("origin kafka data");

        // 2.推送kafka
        String targetTopicName = "motorVehicle2";
        String bootstrap2 = properties.getProperty("target.bootstrap.servers");
        FlinkKafkaProducer010<JSONObject> kafkaProducer = new FlinkKafkaProducer010(bootstrap2, targetTopicName, new KafkaMsgSchema());
        originDataStream.addSink(kafkaProducer).setParallelism(1).name("sync filter data");

        env.execute("kafka-sync-kafka");
    }
}
