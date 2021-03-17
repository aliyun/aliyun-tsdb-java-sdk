package com.aliyun.hitsdb.client;

import com.aliyun.hitsdb.client.value.response.LastDataValue;
import com.aliyun.hitsdb.client.value.response.MultiFieldQueryLastResult;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * @author cuiyuan
 * @date 2021/3/24 12:21 下午
 */
public class TestLastResultTimestampReverse {

    @Test
    public void testMultiValueTimestampReverse() {
        MultiFieldQueryLastResult multiFieldQueryLastResult = new MultiFieldQueryLastResult();

        List<Object> t1 = new ArrayList<Object>();
        List<Object> t2 = new ArrayList<Object>();
        List<Object> t3 = new ArrayList<Object>();
        t1.add(1L);
        t2.add(2L);
        t3.add(3L);

        List<List<Object>> values = new ArrayList<List<Object>>();
        values.add(t1);
        values.add(t2);
        values.add(t3);

        multiFieldQueryLastResult.setValues(values);

        long timestamp = 1;
        for (List<Object> value :multiFieldQueryLastResult.getValues()) {
            Assert.assertEquals(timestamp, value.get(0));
            timestamp++;
        }

        TSDBClient.reverseMultiValueTimestamp(multiFieldQueryLastResult);

        Assert.assertEquals(3, multiFieldQueryLastResult.getValues().size());
        timestamp = 3;
        for (List<Object> value :multiFieldQueryLastResult.getValues()) {
            Assert.assertEquals(timestamp, value.get(0));
            timestamp--;
        }
    }

    @Test
    public void testSingleValueTimestampReverse() {
        LastDataValue lastDataValue = new LastDataValue();
        LinkedHashMap<Long, Object> dps = new LinkedHashMap<Long, Object>();
        dps.put(1L, 1L);
        dps.put(2L, 2L);
        dps.put(3L, 3L);
        lastDataValue.setDps(dps);
        long timestamp = 1;
        for (Map.Entry<Long, Object> longObjectEntry : lastDataValue.getDps().entrySet()) {
            Assert.assertEquals(timestamp, longObjectEntry.getKey().longValue());
            Assert.assertEquals(timestamp, longObjectEntry.getValue());
            timestamp++;
        }

        TSDBClient.reverseSingleValueTimestamp(lastDataValue);

        //after reverse
        Assert.assertEquals(3, lastDataValue.getDps().entrySet().size());
        timestamp = 3;
        for (Map.Entry<Long, Object> longObjectEntry : lastDataValue.getDps().entrySet()) {
            Assert.assertEquals(timestamp, longObjectEntry.getKey().longValue());
            Assert.assertEquals(timestamp, longObjectEntry.getValue());
            timestamp--;
        }
    }
}
