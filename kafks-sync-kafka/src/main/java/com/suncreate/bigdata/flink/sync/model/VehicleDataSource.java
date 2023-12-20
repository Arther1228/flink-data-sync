package com.suncreate.bigdata.flink.sync.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author yangliangchuang 2023/9/1 18:26
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class VehicleDataSource {

    private String dataSource3;
    private Long totalCount;
    private String dateTime;
}
