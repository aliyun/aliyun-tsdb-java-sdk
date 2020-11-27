package com.aliyun.hitsdb.client.value.request;

import com.alibaba.fastjson.JSON;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class LastPointSubQueryTest {
    @Test
    public void testLastPointSubQuerySerialization() {
        {
            LastPointSubQuery subQuery = LastPointSubQuery
                    .builder("fake.metric")
                    .hint(new HashMap<String, Map<String, Integer>>(){{
                        put("tagk", new HashMap<String, Integer>(){{
                            put("iotid", 1);
                        }});
                    }})
                    .build();
            String serializedString = JSON.toJSONString(subQuery);
            System.out.println(serializedString);

            Assert.assertTrue(serializedString.equals("{\"metric\":\"fake.metric\",\"hint\":{\"tagk\":{\"iotid\":1}}}"));

            LastPointSubQuery newSubQuery = JSON.parseObject(serializedString, LastPointSubQuery.class);

            Assert.assertTrue(newSubQuery.getMetric().equals(subQuery.getMetric()));
            Assert.assertNotNull(newSubQuery.getHint());
            Assert.assertNotNull(newSubQuery.getHint().get("tagk"));

            Assert.assertEquals(subQuery.getHint().get("tagk").get("iotid"), newSubQuery.getHint().get("tagk").get("iotid"));
        }

        {
            LastPointSubQuery subQuery = LastPointSubQuery
                    .builder("fake.metric")
                    .hint(null)
                    .build();
            String serializedString = JSON.toJSONString(subQuery);
            System.out.println(serializedString);

            Assert.assertTrue(serializedString.equals("{\"metric\":\"fake.metric\"}"));

            LastPointSubQuery newSubQuery = JSON.parseObject(serializedString, LastPointSubQuery.class);

            Assert.assertTrue(newSubQuery.getMetric().equals(subQuery.getMetric()));
            Assert.assertNull(newSubQuery.getHint());
        }

        {
            try {
                LastPointSubQuery subQuery = LastPointSubQuery
                        .builder("fake.metric")
                        .hint(new HashMap<String, Map<String, Integer>>())
                        .build();
                Assert.fail("the LastPointSubQuery object should not be created successfully");
            } catch (IllegalArgumentException iaex) {
                System.err.println(iaex.getMessage());
            } catch (Exception ex) {
                Assert.fail("unexpected exception: " + ex.getClass().getName());
            }
        }
    }
}
