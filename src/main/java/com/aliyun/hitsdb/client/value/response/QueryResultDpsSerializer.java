package com.aliyun.hitsdb.client.value.response;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.DefaultJSONParser;
import com.alibaba.fastjson.parser.deserializer.ObjectDeserializer;
import com.alibaba.fastjson.serializer.JSONSerializer;
import com.alibaba.fastjson.serializer.ObjectSerializer;
import com.aliyun.hitsdb.client.value.request.ByteArrayValue;
import com.aliyun.hitsdb.client.value.request.ComplexValue;
import com.aliyun.hitsdb.client.value.request.GeoPointValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * @author cuiyuan
 * @date 2020/8/7 4:35 下午
 */
public class QueryResultDpsSerializer implements ObjectSerializer, ObjectDeserializer {
    static final Logger log = LoggerFactory.getLogger(QueryResultDpsSerializer.class);

    @Override
    public LinkedHashMap<Long, Object> deserialze(DefaultJSONParser parser, Type type, Object fieldName) {
        Object parse = parser.parse();
        if (parse == null) {
            return null;
        }
        TreeMap<Long, Object> retMap = new TreeMap<Long, Object>();
        for (Map.Entry<String, Object> entry : ((JSONObject) parse).entrySet()) {
            if (entry.getValue() instanceof JSONObject) {
                JSONObject jsonObject = (JSONObject) entry.getValue();
                if (ComplexValue.isJsonObjectTypeMatch(jsonObject)) {
                    if (ByteArrayValue.isJsonObjectTypeMatch(jsonObject)) {
                        ByteArrayValue bv = JSON.parseObject(jsonObject.toJSONString(), ByteArrayValue.class);
                        entry.setValue(bv.decode());
                    } else if (GeoPointValue.isJsonObjectTypeMatch(jsonObject)) {
                        GeoPointValue gp = JSON.parseObject(jsonObject.toJSONString(), GeoPointValue.class);
                        entry.setValue(gp);
                    } else {
                        log.error("Illegal value type {}", jsonObject);
                        throw new IllegalArgumentException("Illegal value type " + jsonObject);
                    }
                }
            }
            retMap.put(Long.parseLong(String.valueOf(entry.getKey())), entry.getValue());
        }

        return new LinkedHashMap<Long, Object>(retMap);
    }


    @Override
    public void write(JSONSerializer serializer, Object object, Object fieldName, Type fieldType, int features) throws IOException {
        LinkedHashMap<Long, Object> map = (LinkedHashMap<Long, Object>) object;
        LinkedHashMap<Long, Object> original = new LinkedHashMap<Long, Object>();
        for (Map.Entry<Long, Object> entry : map.entrySet()) {
            if (entry.getValue() instanceof byte[]) {
                Object value = entry.getValue();
                ByteArrayValue bv = new ByteArrayValue((byte[]) value);
                entry.setValue(bv);
                original.put(entry.getKey(), value);
            }
        }
        serializer.write(map);
        for (Map.Entry<Long, Object> entry : original.entrySet()) {
            map.put(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public int getFastMatchToken() {
        return 0;
    }
}
