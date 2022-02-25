package com.aliyun.hitsdb.client.util;

import com.aliyun.hitsdb.client.http.HttpAPI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.*;

public class HealthManager {

    private static final Logger LOG = LoggerFactory.getLogger(HealthManager.class);

    private ConcurrentMap<String, HealthWatcher> watchers = new ConcurrentHashMap();

    private ScheduledExecutorService executorService;

    private int intervalSeconds = 2;


    public HealthManager() {
        this.executorService = Executors.newSingleThreadScheduledExecutor();
    }


    public void start() {
        if (this.intervalSeconds > 0) {
            this.executorService.scheduleWithFixedDelay(new WatchRunnable(),
                    intervalSeconds, intervalSeconds, TimeUnit.SECONDS);
        }
    }

    public void stop() {
        this.executorService.shutdown();
    }

    public void setIntervalSeconds(int intervalSeconds) {
        this.intervalSeconds = intervalSeconds;
    }

    public void watch(final String host, final HealthWatcher watcher) {
        this.watchers.put(host, watcher);
    }


    public void unWatch(final String host) {
        this.watchers.remove(host);
    }

    public void unWatchAll() {
        this.watchers.clear();
    }


    private final class WatchRunnable implements Runnable {

        @Override
        public void run() {
            try {
                LOG.info("start to run health check");
                long start = System.currentTimeMillis();
                for (final Map.Entry<String, HealthWatcher> entry : watchers.entrySet()) {
                    final String host = entry.getKey();
                    final HealthWatcher watcher = entry.getValue();
                    watcher.health(host, healthCheck(host));
                }
                LOG.info("finished run health check,cost {}ms", System.currentTimeMillis() - start);
            } catch (Exception e) {
                LOG.error("when run health check error", e);
            }
        }
    }


    public static final String OK = "OK";

    public boolean healthCheck(String host) {
        if (!host.startsWith("http")) {
            host = "http://" + host;
        }
        String url = host + HttpAPI.HEALTH;
        if (LOG.isDebugEnabled()) {
            LOG.debug("start to check {} ", host);
        }
        try {
            String result = HttpUtil.get(url);
            if (result.isEmpty()) {
                return false;
            }
            // 健康检查
            if (OK.equals(result.toUpperCase())) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("the host:{} work well", host);
                }
                return true;
            } else {
                LOG.warn("the host :{} is not work well", host);
                return false;
            }
        } catch (Exception e) {
            LOG.warn("the host {} maybe not health, so will be remove", host, e);
        }
        return false;
    }
}
