package com.aliyun.hitsdb.client.value;

import com.aliyun.hitsdb.client.TSDB;
import com.aliyun.hitsdb.client.TSDBClientFactory;
import com.aliyun.hitsdb.client.TSDBConfig;
import org.junit.Assert;
import org.junit.Test;

public class TSDBClientFactoryTest {
    @Test
    public void testConnectNonexistAddress() {
        {
            int initialActiveThreadCount = Thread.activeCount();
            int deviation = 3;    // a deviation to tolerate some unexpected scenario

            int i = 0;
            while (i < 100) {
                i++;
                try {
                    TSDB client = TSDBClientFactory.connect("127.0.0.1", 65534);     // it's an unreachable address
                    if (client != null) {
                        Assert.fail("the connect should not succeed");
                    }
                } catch (Exception hex) {
                    ;
                    ;
                }

                try {
                    Thread.sleep(50);
                } catch (Exception ex) {
                    ;
                    ;
                }
            }

            int currentActiveThreadCount = Thread.activeCount();

            Assert.assertTrue(String.format("current thread count: %d, initial thread count: %d", currentActiveThreadCount, initialActiveThreadCount), currentActiveThreadCount >= initialActiveThreadCount - deviation);
            Assert.assertTrue(String.format("current thread count: %d, initial thread count: %d", currentActiveThreadCount, initialActiveThreadCount), currentActiveThreadCount <= initialActiveThreadCount + deviation);
        }

        {
            int initialActiveThreadCount = Thread.activeCount();
            int deviation = 3;    // a deviation to tolerate some unexpected scenario

            int i = 0;
            while (i < 100) {
                i++;
                try {
                    TSDBConfig config = TSDBConfig.address("127.0.0.1", 65534).asyncPut(true).config();
                    TSDB client = TSDBClientFactory.connect(config);     // it's an unreachable address
                    if (client != null) {
                        Assert.fail("the connect should not succeed");
                    }
                } catch (Exception hex) {
                    ;
                    ;
                }

                try {
                    Thread.sleep(50);
                } catch (Exception ex) {
                    ;
                    ;
                }
            }

            int currentActiveThreadCount = Thread.activeCount();

            Assert.assertTrue(String.format("current thread count: %d, initial thread count: %d", currentActiveThreadCount, initialActiveThreadCount), currentActiveThreadCount >= initialActiveThreadCount - deviation);
            Assert.assertTrue(String.format("current thread count: %d, initial thread count: %d", currentActiveThreadCount, initialActiveThreadCount), currentActiveThreadCount <= initialActiveThreadCount + deviation);
        }
    }
}
