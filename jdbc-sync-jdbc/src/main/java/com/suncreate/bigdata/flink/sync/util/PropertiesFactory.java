package com.suncreate.bigdata.flink.sync.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;

public class PropertiesFactory implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(PropertiesFactory.class);

    private static PropertiesFactory propertiesFactory = new PropertiesFactory();

    private static Properties properties = null;

    private PropertiesFactory() {
    }

    public static Properties getProperties() {
        if (properties != null) {
            return properties;
        }
        properties = new Properties();
        //String path = "/opt/hadoopclient/Flink/flink/conf/kafka-sync-kafka.properties";
        String path = "conf/jdbc-sync-jdbc.properties";
        try {
            properties.load(new FileInputStream(new File(path)));
        } catch (IOException e) {
            LOGGER.error("load properties failed!", e);
        }
        return properties;
    }
}
