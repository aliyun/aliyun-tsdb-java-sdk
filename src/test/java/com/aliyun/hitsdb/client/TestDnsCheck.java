/*
 * Copyright Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.aliyun.hitsdb.client;

import com.aliyun.hitsdb.client.value.request.Point;

public class TestDnsCheck {

    @org.junit.Test
    public void test() {
        TSDBConfig config = TSDBConfig
                .address("ld-8vb4hz013p9llu9ib-proxy-tsdb-pub.lindorm.rds.aliyuncs.com", 8242)
                .dnsCheckEnable(5)
                .batchPutSize(50)
                .batchPutRetryCount(3)
                .multiFieldBatchPutConsumerThreadCount(2)
                .config();
        TSDBClient client = new TSDBClient(config);
        while (true) {
            Point point = Point.metric("hello").tag("tagk1", "tagv1").timestamp(4294969).value(123.456).build();
            client.put(point);
        }
    }
}
