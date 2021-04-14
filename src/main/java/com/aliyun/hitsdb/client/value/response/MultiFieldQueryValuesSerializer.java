package com.aliyun.hitsdb.client.value.response;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.DefaultJSONParser;
import com.alibaba.fastjson.parser.deserializer.ObjectDeserializer;
import com.alibaba.fastjson.serializer.JSONSerializer;
import com.alibaba.fastjson.serializer.ObjectSerializer;
import com.aliyun.hitsdb.client.util.Pair;
import com.aliyun.hitsdb.client.value.request.ByteArrayValue;
import com.aliyun.hitsdb.client.value.request.ComplexValue;
import com.aliyun.hitsdb.client.value.request.GeoPointValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

/**
 * @author cuiyuan
 * @date 2020/8/5 10:13 上午
 */
public class MultiFieldQueryValuesSerializer implements ObjectSerializer, ObjectDeserializer {

    static private final Logger log = LoggerFactory.getLogger(MultiFieldQueryValuesSerializer.class);

    @Override
    public <T> T deserialze(DefaultJSONParser parser, Type type, Object fieldName) {
        List<List> objectsList = parser.parseArray(List.class);
        for (int i = 0; i < objectsList.size(); i++) {
            List<Object> objects = (List<Object>) objectsList.get(i);
            boolean changed = false;
            for (int j = 0; j < objects.size(); j++) {
                if (objects.get(j) instanceof JSONObject) {
                    JSONObject jsonObject = (JSONObject) (objects.get(j));
                    if (ComplexValue.isJsonObjectTypeMatch(jsonObject)) {
                        String valueType = jsonObject.getString(ByteArrayValue.TypeKey);
                        if (ByteArrayValue.TypeValue.equals(valueType)){
                            ByteArrayValue bv = JSON.parseObject(jsonObject.toJSONString(), ByteArrayValue.class);
                            objects.set(j, bv.decode());
                            changed = true;
                        } else if (GeoPointValue.TypeValue.equals(valueType)) {
                            GeoPointValue gp = JSON.parseObject(jsonObject.toJSONString(), GeoPointValue.class);
                            objects.set(j, gp);
                            changed = true;
                        } else{
                            log.error("Illegal value type {}", valueType);
                            throw new IllegalArgumentException("Illegal value type " + valueType);
                        }
                    }
                }
            }
            if(changed) {
                objectsList.set(i, objects);
            }
        }
        return (T) objectsList;
    }

    @Override
    public int getFastMatchToken() {
        return 0;
    }

    @Override
    public void write(JSONSerializer serializer, Object object, Object fieldName, Type fieldType, int features) throws IOException {
        List<Pair<Integer,Integer>> indexes = new ArrayList<Pair<Integer,Integer>>();
        List<Object> values = new ArrayList<Object>();
        List<List<Object>> lists = (List<List<Object>>) object;
        for (int i = 0; i < lists.size(); i++) {
            List<Object> list = lists.get(i);
            for (int j = 0; j < list.size(); j++) {
                Object o = list.get(j);
                if (o instanceof byte[]) {
                    ByteArrayValue bv = new ByteArrayValue((byte[]) o);
                    list.set(j, bv);
                    indexes.add(new Pair<Integer,Integer>(i,j));
                    values.add(o);
                }
            }
        }
        serializer.write(lists);
        for (int i = 0; i < indexes.size(); i++) {
            Pair<Integer,Integer> index = indexes.get(i);
            lists.get(index.getKey()).set(index.getValue(),values.get(i));
        }
    }
}
