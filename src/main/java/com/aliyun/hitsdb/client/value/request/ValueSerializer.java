package com.aliyun.hitsdb.client.value.request;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.DefaultJSONParser;
import com.alibaba.fastjson.parser.deserializer.ObjectDeserializer;
import com.alibaba.fastjson.serializer.JSONSerializer;
import com.alibaba.fastjson.serializer.ObjectSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;

/**
 * @author cuiyuan
 * @date 2020/8/4 11:30 上午
 */
public class ValueSerializer implements ObjectSerializer, ObjectDeserializer {

    private static final Logger log = LoggerFactory.getLogger(ValueSerializer.class);

    @Override
    public void write(JSONSerializer jsonSerializer, Object o, Object o1, Type type, int i) throws IOException {
        if (o instanceof byte[]) {
            ByteArrayValue bv = new ByteArrayValue((byte[]) o);
            jsonSerializer.write(bv);
            return;
        }
        jsonSerializer.write(o);
    }

    @Override
    public Object deserialze(DefaultJSONParser parser, Type type, Object o) {
        Object parse = parser.parse();
        if (parse == null) {
            return null;
        }
        if (parse instanceof JSONObject) {
            JSONObject jsonObject = (JSONObject) parse;
            if (ComplexValue.isJsonObjectTypeMatch(jsonObject)) {
                String valueType = jsonObject.getString(ByteArrayValue.TypeKey);
                if (ByteArrayValue.TypeValue.equals(valueType)) {
                    ByteArrayValue bv = JSON.parseObject(jsonObject.toJSONString(), ByteArrayValue.class);
                    return bv.decode();
                } else if (GeoPointValue.TypeValue.equals(valueType)) {
                    GeoPointValue gp = JSON.parseObject(jsonObject.toJSONString(), GeoPointValue.class);
                    return gp;
                } else {
                    log.error("Illegal value type {}", valueType);
                    throw new IllegalArgumentException("Illegal value type " + valueType);
                }
            }
        }
        return parse;
    }

    @Override
    public int getFastMatchToken() {
        return 0;
    }
}
