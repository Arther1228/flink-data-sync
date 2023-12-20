package com.suncreate.bigdata.flink.sync.utils;

import com.suncreate.logback.elasticsearch.metric.DataType;
import com.suncreate.logback.elasticsearch.metric.SinkType;
import com.suncreate.logback.elasticsearch.metric.SourceType;
import com.suncreate.logback.elasticsearch.util.MetricUtil;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.HashMap;

/**
 * @author admin
 */
public class LogUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(LogUtil.class);
    private static final String doc = "doc";

    private static RestHighLevelClient esClient;

    static {
        esClient = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("34.8.8.91", 24102, "http"),
                        new HttpHost("34.8.8.92", 24102, "http"),
                        new HttpHost("34.8.8.93", 24102, "http"),
                        new HttpHost("34.8.8.94", 24102, "http"),
                        new HttpHost("34.8.8.95", 24102, "http"),
                        new HttpHost("34.8.8.96", 24102, "http"),
                        new HttpHost("34.8.8.97", 24102, "http"),
                        new HttpHost("34.8.8.98", 24102, "http"),
                        new HttpHost("34.8.8.122", 24102, "http"),
                        new HttpHost("34.8.8.123", 24102, "http"),
                        new HttpHost("34.8.8.124", 24102, "http"),
                        new HttpHost("34.8.8.125", 24102, "http"),
                        new HttpHost("34.8.8.126", 24102, "http"),
                        new HttpHost("34.8.8.127", 24102, "http"),
                        new HttpHost("34.8.8.128", 24102, "http"),
                        new HttpHost("34.8.8.129", 24102, "http")));
    }


    public static void log2LogbackInfo(String info) {
        HashMap<String, Object> logMap = new HashMap<>();
        logMap.put("data_source", "kafka-sync-kafka");
        logMap.put("data_catalog", "vehicle");
        logMap.put("message", info);
        logMap.put("level", "INFO");
        logMap.put("proc_time", DateUtil.nowDate());
        send2ES(esClient, logMap);
    }

    public static void log2LogbackError(String info) {
        HashMap<String, Object> logMap = new HashMap<>();
        logMap.put("data_source", "kafka-sync-kafka");
        logMap.put("data_catalog", "vehicle");
        logMap.put("message", info);
        logMap.put("level", "ERROR");
        logMap.put("proc_time", DateUtil.nowDate());
        send2ES(esClient, logMap);
    }


    public static void log2ES4Kafka(String procPhase, String procStatus, Integer count) {
        HashMap<String, Object> logMap = (HashMap<String, Object>) MetricUtil.getMap("kafka-sync-kafka", "kafka-sync-kafka", DataType.struct_data.toString(), procPhase,
                procStatus, SourceType.kafka.toString(), SinkType.kafka.toString(), count);

        send2ES(esClient, logMap);
    }


    private static void send2ES(RestHighLevelClient client, HashMap<String, Object> logMap) {
        logMap.put("@timestamp", System.currentTimeMillis());
        String index = "logback-" + DateFormatUtils.format(new Date(), "yyyy-MM");
        IndexRequest indexRequest = new IndexRequest(index, doc)
                .source(logMap);
        client.indexAsync(indexRequest, new ActionListener<IndexResponse>() {
            @Override
            public void onResponse(IndexResponse indexResponse) {
                LOGGER.warn("send log 2 es success！");
            }

            @Override
            public void onFailure(Exception e) {
                LOGGER.warn("send log 2 es failed！");
            }
        });
    }


}
