package com.suncreate.bigdata.flink.sync.model;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.Charset;

/**
 * @author HP
 */
public class KafkaMsgSchema implements DeserializationSchema<JSONObject>, SerializationSchema<JSONObject> {
    private static final long serialVersionUID = 1L;
    private transient Charset charset;

    public KafkaMsgSchema() {
        // 默认UTF-8编码
        this(Charset.forName("UTF-8"));
    }

    public KafkaMsgSchema(Charset charset) {
        this.charset = Preconditions.checkNotNull(charset);
    }

    public Charset getCharset() {
        return this.charset;
    }

    @Override
    public JSONObject deserialize(byte[] message) {
        try {
            // 将Kafka的消息反序列化为java对象
            return JSONObject.parseObject(new String(message, charset), JSONObject.class);
        } catch (Exception e) {
            return new JSONObject();
        }
    }

    @Override
    public boolean isEndOfStream(JSONObject nextElement) {
        // 流永远不结束
        return false;
    }

    @Override
    public byte[] serialize(JSONObject element) {
        // 将java对象序列化为Kafka的消息
        return JSONObject.toJSONString(element).getBytes(this.charset);
    }

    @Override
    public TypeInformation<JSONObject> getProducedType() {
        return TypeInformation.of(JSONObject.class);
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        out.writeUTF(this.charset.name());
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        String charsetName = in.readUTF();
        this.charset = Charset.forName(charsetName);
    }
}
