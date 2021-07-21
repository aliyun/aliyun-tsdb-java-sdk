package com.aliyun.hitsdb.client.value;

import com.aliyun.hitsdb.client.HAPolicy;
import com.aliyun.hitsdb.client.TSDBConfig;
import org.junit.Assert;
import org.junit.Test;

public class TestConfig {
    @Test
    public void testCopy() {
        {
            String user = "testuser1", password = "password";

            TSDBConfig config = TSDBConfig
                    .address("host1", 8242)
                    .basicAuth(user, password)
                    .httpConnectionLiveTime(1800)
                    .batchPutSize(50)
                    .batchPutRetryCount(3)
                    .multiFieldBatchPutConsumerThreadCount(2)
                    .config();

            TSDBConfig configCopy = config.copy("127.0.0.1", 3002);

            Assert.assertFalse(config.getHost().equals(configCopy.getHost()));
            Assert.assertNotEquals(config.getPort(), configCopy.getPort());

            Assert.assertEquals(config.getBatchPutSize(), configCopy.getBatchPutSize());
            Assert.assertEquals(config.getBatchPutRetryCount(), configCopy.getBatchPutRetryCount());
            Assert.assertEquals(config.getMultiFieldBatchPutConsumerThreadCount(),
                    configCopy.getMultiFieldBatchPutConsumerThreadCount());
            Assert.assertEquals(config.getBatchPutTimeLimit(), configCopy.getBatchPutTimeLimit());
            Assert.assertEquals(config.getHttpConnectionLiveTime(), configCopy.getHttpConnectionLiveTime());

            Assert.assertTrue(config.getAuthType().equals(configCopy.getAuthType()));
            Assert.assertTrue(config.getTsdbUser().equals(configCopy.getTsdbUser()));
            Assert.assertTrue(config.getBasicPwd().equals(configCopy.getBasicPwd()));

            Assert.assertEquals(config.getHttpConnectionPool(), configCopy.getHttpConnectionPool());
            Assert.assertEquals(config.getHttpConnectionRequestTimeout(), configCopy.getHttpConnectionRequestTimeout());
            Assert.assertEquals(config.getHttpConnectTimeout(), configCopy.getHttpConnectTimeout());
            Assert.assertEquals(config.getHttpKeepaliveTime(), configCopy.getHttpKeepaliveTime());
            Assert.assertEquals(config.getHttpSocketTimeout(), configCopy.getHttpSocketTimeout());

            Assert.assertNull(configCopy.getHAPolicy());
        }

        {
            String user = "testuser1", password = "password";

            HAPolicy policy = HAPolicy.addSecondaryCluster("host2", 8242)
                    .setRetryRule(HAPolicy.RetryRule.SecondaryPreferred)
                    .setRetryTimes(0)
                    .build();

            TSDBConfig config = TSDBConfig
                    .address("host1", 8242)
                    .basicAuth(user, password)
                    .httpConnectionLiveTime(1800)
                    .batchPutSize(50)
                    .batchPutRetryCount(3)
                    .multiFieldBatchPutConsumerThreadCount(2)
                    .addHAPolicy(policy)
                    .httpConnectionPool(10)
                    .httpConnectionRequestTimeout(120)
                    .httpConnectTimeout(60)
                    .httpKeepaliveTime(60)
                    .httpSocketTimeout(180)
                    .config();

            TSDBConfig configCopy = config.copy("127.0.0.1", 3002);

            Assert.assertFalse(config.getHost().equals(configCopy.getHost()));
            Assert.assertNotEquals(config.getPort(), configCopy.getPort());

            Assert.assertEquals(config.getBatchPutSize(), configCopy.getBatchPutSize());
            Assert.assertEquals(config.getBatchPutRetryCount(), configCopy.getBatchPutRetryCount());
            Assert.assertEquals(config.getMultiFieldBatchPutConsumerThreadCount(),
                    configCopy.getMultiFieldBatchPutConsumerThreadCount());
            Assert.assertEquals(config.getBatchPutTimeLimit(), configCopy.getBatchPutTimeLimit());
            Assert.assertEquals(config.getHttpConnectionLiveTime(), configCopy.getHttpConnectionLiveTime());

            Assert.assertTrue(config.getAuthType().equals(configCopy.getAuthType()));
            Assert.assertTrue(config.getTsdbUser().equals(configCopy.getTsdbUser()));
            Assert.assertTrue(config.getBasicPwd().equals(configCopy.getBasicPwd()));

            Assert.assertEquals(config.getHttpConnectionPool(), configCopy.getHttpConnectionPool());
            Assert.assertEquals(config.getHttpConnectionRequestTimeout(), configCopy.getHttpConnectionRequestTimeout());
            Assert.assertEquals(config.getHttpConnectTimeout(), configCopy.getHttpConnectTimeout());
            Assert.assertEquals(config.getHttpKeepaliveTime(), configCopy.getHttpKeepaliveTime());
            Assert.assertEquals(config.getHttpSocketTimeout(), configCopy.getHttpSocketTimeout());

            Assert.assertTrue(configCopy.getHAPolicy().equals(config.getHAPolicy()));
        }
    }
}
