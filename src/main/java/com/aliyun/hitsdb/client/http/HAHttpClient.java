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
package com.aliyun.hitsdb.client.http;

import com.alibaba.fastjson.JSONObject;
import com.aliyun.hitsdb.client.Config;
import com.aliyun.hitsdb.client.HAPolicy;
import com.aliyun.hitsdb.client.callback.http.HttpResponseCallbackFactory;
import com.aliyun.hitsdb.client.util.Objects;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class HAHttpClient {
    private HAPolicy haPolicy;
    private HttpClient primaryClient;
    private HttpClient secondaryClient;
    private HealthCheckClient healthCheckClient;
    private static final String EMPTY_HOLDER = new JSONObject().toJSONString();
    private static final String HEALTH_API = "/api/health";


    public void setCurrentDatabase(String database) {
        primaryClient.setCurrentDatabase(database);
        if (secondaryClient!=null) {
            secondaryClient.setCurrentDatabase(database);
        }
    }

    private HAHttpClient() {
    }

    public static HAHttpClient createHAHttpClient(Config config) {
        HAHttpClient haHttpClient = new HAHttpClient();
        haHttpClient.haPolicy = config.getHAPolicy();
        haHttpClient.primaryClient = HttpClientFactory.createHttpClient(config);

        if (haHttpClient.getHaPolicy().hasSecondaryCluster()) {
            Config secondaryConfig = config.copy(config.getHAPolicy().getSecondaryHost(), config.getHAPolicy().getSecondaryPort());
            haHttpClient.secondaryClient = HttpClientFactory.createHttpClient(secondaryConfig);

            if (haHttpClient.getHaPolicy().hasHealthCheckRule()) {
                haHttpClient.healthCheckClient = new HealthCheckClient(haHttpClient);
            }
        }

        return haHttpClient;
    }

    public void start() {
        Objects.requireNonNull(primaryClient, "primary client should not be null");
        primaryClient.start();
        if (secondaryClient!=null){
            secondaryClient.start();
        }
        if (healthCheckClient!=null) {
            healthCheckClient.start();
        }
    }

    public void close() throws IOException {
        primaryClient.close(true);
        if (secondaryClient != null) {
            secondaryClient.close(true);
        }
        if (healthCheckClient != null) {
            healthCheckClient.close();
        }
    }

    public void checkConnection() {
        primaryClient.post(HEALTH_API, EMPTY_HOLDER);
        if (secondaryClient != null) {
            secondaryClient.post(HEALTH_API, EMPTY_HOLDER);
        }
    }

    public HttpResponseCallbackFactory getHttpResponseCallbackFactory() {
        return this.primaryClient.getHttpResponseCallbackFactory();
    }

    public HAPolicy getHaPolicy() {
        return haPolicy;
    }

    public HttpClient getPrimaryClient() {
        return primaryClient;
    }

    public HttpClient getReadClient(int retryTimes) {
        switch (haPolicy.getReadRule()) {
            case Primary:
                return primaryClient;
            case Secondary:
                return secondaryClient;
            case SecondaryPreferred:
                if (retryTimes <= haPolicy.getQueryRetryTimes()) {
                    return secondaryClient;
                }
                return primaryClient;
            case PrimaryPreferred:
                if (retryTimes <= haPolicy.getQueryRetryTimes()) {
                    return primaryClient;
                }
                return secondaryClient;
            case HealthCheck:
                return healthCheckClient.getAliveClient();
            default:
                throw new IllegalArgumentException("Unknown Read Policy");
        }
    }

    public HttpClient getWriteClient() {
        switch (haPolicy.getWriteRule()) {
            case Primary:
                return primaryClient;
            case HealthCheck:
                return healthCheckClient.getAliveClient();
            default:
                throw new IllegalArgumentException("Unknown Read Policy");
        }
    }

    public static class HealthCheckClient {

        private static final int RetryNum = 3;
        private static final Logger LOGGER = LoggerFactory.getLogger(HealthCheckClient.class);
        private final HAHttpClient haHttpClient;
        private final AtomicReference<HttpClient> aliveClient;
        private final OkHttpClient healthCheckClient;
        private final int healthCheckInterval;
        private final ScheduledExecutorService executorService;
        private ScheduledFuture<?> healthCheckHandle;

        private HttpClient getPrimaryClient() {
            return haHttpClient.primaryClient;
        }

        private HttpClient getSecondaryClient() {
            return haHttpClient.secondaryClient;
        }

        public HealthCheckClient(HAHttpClient haHttpClient) {
            Objects.requireNonNull(haHttpClient.primaryClient, "primary client can't be null");
            this.haHttpClient = haHttpClient;
            this.aliveClient = new AtomicReference<HttpClient>(haHttpClient.primaryClient);
            int timeout = haHttpClient.getHaPolicy().getHealthCheckTimeout();
            this.healthCheckInterval = haHttpClient.getHaPolicy().getHealthCheckInterval();
            this.executorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
                @Override
                public Thread newThread(@NotNull Runnable r) {
                    Thread t = Executors.defaultThreadFactory().newThread(r);
                    t.setDaemon(true);
                    return t;
                }
            });
            healthCheckClient = new OkHttpClient.Builder()
                    .callTimeout(timeout, TimeUnit.SECONDS)
                    .build();
        }

        public void start() {
            this.healthCheckHandle = this.executorService.scheduleWithFixedDelay(new Runnable() {
                @Override
                public void run() {
                    checkActive();
                }
            }, this.healthCheckInterval, this.healthCheckInterval, TimeUnit.SECONDS);
        }

        public void close() {
            healthCheckHandle.cancel(true);
            executorService.shutdown();
        }

        private void checkActive() {
            try {
                if (retryCheckConnection(getPrimaryClient())) {
                    if (getAliveClient() != getPrimaryClient()) {
                        LOGGER.info("client switch to primary");
                    }
                    setAliveClient(getPrimaryClient());
                } else {
                    if (retryCheckConnection(getSecondaryClient())) {
                        if (getAliveClient() != getSecondaryClient()) {
                            LOGGER.info("client switch to secondary");
                        }
                        setAliveClient(getSecondaryClient());
                    } else {
                        LOGGER.error("primary and secondary all dead");
                    }
                }
            } catch (Throwable t) {
                LOGGER.error("error occurred in health check",t);
            }
        }

        public HttpClient getAliveClient() {
            return aliveClient.get();
        }

        public void setAliveClient(HttpClient aliveClient) {
            this.aliveClient.set(aliveClient);
        }

        private boolean checkConnection(HttpClient httpClient) {
            String healthCheckAddr = String.format("http://%s/api/health", httpClient.getHttpAddressManager().getAddress());
            try {
                Request request = new Request.Builder().url(healthCheckAddr).build();
                Response response = healthCheckClient.newCall(request).execute();
                return response.isSuccessful();
            } catch (Exception e) {
                LOGGER.warn("health check {} failed {}", healthCheckAddr, e);
                return false;
            }
        }

        private boolean retryCheckConnection(HttpClient httpClient) {
            long backOffTime = 0;
            for (int i = 0; i < RetryNum; i++) {
                if (checkConnection(httpClient)) {
                    return true;
                }

                backOffTime = backOffTime + (i + 1);
                try {
                    TimeUnit.SECONDS.sleep(backOffTime); //if check fail, sleep time back off, 1/3/6
                } catch (InterruptedException ignored) {
                }
            }
            LOGGER.warn("health check failed after retry: {}", httpClient.getHttpAddressManager().getAddress());
            return false;
        }
    }
}


