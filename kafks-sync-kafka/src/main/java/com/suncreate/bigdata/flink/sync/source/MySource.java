package com.suncreate.bigdata.flink.sync.source;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * 模拟数据
 */
public class MySource implements SourceFunction<Tuple2<String, Long>> {
    private boolean flag = true;
    private static String message = "{\n" +
            "  \"create_time3\": \"20220628083307\",\n" +
            "  \"data_source3\": \"XLSK\",\n" +
            "  \"device_id\": \"34010347001190310046\",\n" +
            "  \"direction\": \"4\",\n" +
            "  \"has_plate\": true,\n" +
            "  \"info_kind\": \"1\",\n" +
            "  \"lane_no\": 3,\n" +
            "  \"left_top_x\": 1600,\n" +
            "  \"left_top_y\": 2,\n" +
            "  \"location\": \"31.881198,117.281956\",\n" +
            "  \"motor_vehicle_id\": \"340103470011903100460220220628083305037430200001\",\n" +
            "  \"pass_time\": \"20220628083305\",\n" +
            "  \"plate_class\": \"02\",\n" +
            "  \"plate_color\": \"5\",\n" +
            "  \"plate_no\": \"皖AV998C\",\n" +
            "  \"region_id2\": \"03\",\n" +
            "  \"right_btm_x\": 3951,\n" +
            "  \"right_btm_y\": 1423,\n" +
            "  \"source_id\": \"34010347001190310046022022062808330503743\",\n" +
            "  \"speed\": \"36\",\n" +
            "  \"storage_url2\": \"/mnt/motorvehicle_2/sckk/2022/06/28/34010347001190310046/F_20220628083305_皖AV998C.jpg\",\n" +
            "  \"storage_url3\": \"/mnt/motorvehicle_2/sckk/2022/06/28/34010347001190310046/P_20220628083305_皖AV998C.jpg\",\n" +
            "  \"storage_url5\": \"/mnt/motorvehicle_2/scrl/2022/06/28/34010347001190310046/R_20220628083305_皖AV998C_0.jpg\",\n" +
            "  \"tollgate_id\": \"34010316001199003015\",\n" +
            "  \"tollgate_name3\": \"庐阳区北一环阜阳路西南角南27米枪1\",\n" +
            "  \"vehicle_class\": \"K33\"\n" +
            "}";

    @Override
    public void run(SourceContext<Tuple2<String, Long>> ctx) throws Exception {
        while (flag) {
            JSONObject json = (JSONObject) JSONObject.parse(message);
            String dataSource = json.getString("data_source3");
            ctx.collect(Tuple2.of(dataSource, 1L));
            Thread.sleep(20);
        }
    }

    @Override
    public void cancel() {
        flag = false;
    }
}