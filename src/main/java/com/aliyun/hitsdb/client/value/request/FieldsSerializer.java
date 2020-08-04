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
import java.util.HashMap;
import java.util.Map;

/**
 * @author cuiyuan
 * @date 2020/8/4 5:23 下午
 */
public class FieldsSerializer implements ObjectSerializer, ObjectDeserializer {

    static final Logger log = LoggerFactory.getLogger(FieldsSerializer.class);

    @Override
    public Map<String, Object> deserialze(DefaultJSONParser parser, Type type, Object fieldName) {
        Object parse = parser.parse();
        if (parse == null) {
            return null;
        }
        if (parse instanceof JSONObject) {
            for (Map.Entry<String, Object> entry : ((JSONObject) parse).entrySet()) {
                if (entry.getValue() instanceof JSONObject) {
                    JSONObject jsonObject = (JSONObject) entry.getValue();
                    if (ByteArrayValue.isJsonObjectTypeMatch(jsonObject)) {
                        String valueType = jsonObject.getString(ByteArrayValue.TypeKey);
                        if (ByteArrayValue.TypeValue.equals(valueType)) {
                            ByteArrayValue bv = JSON.parseObject(jsonObject.toJSONString(), ByteArrayValue.class);
                            entry.setValue(bv.decode());
                        } else {
                            log.error("Illegal value type {}", valueType);
                            throw new IllegalArgumentException("Illegal value type " + valueType);
                        }
                    }
                }
            }
        }
        return (Map<String, Object>) parse;
    }


    @Override
    public void write(JSONSerializer serializer, Object object, Object fieldName, Type fieldType, int features) throws IOException {
        Map<String, Object> map = (Map<String, Object>) object;
        Map<String, Object> original = new HashMap<String, Object>();
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            if (entry.getValue() instanceof byte[]) {
                Object value = entry.getValue();
                ByteArrayValue bv = new ByteArrayValue((byte[]) value);
                entry.setValue(bv);
                original.put(entry.getKey(), value);
            }
        }
        serializer.write(map);
        for (Map.Entry<String, Object> entry : original.entrySet()) {
            map.put(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public int getFastMatchToken() {
        return 0;
    }
}
