package com.aliyun.hitsdb.client.util;

import com.aliyun.hitsdb.client.http.HttpAPI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.Map;
import java.util.concurrent.*;

public class HealthManager {

    private static final Logger LOG = LoggerFactory.getLogger(HealthManager.class);

    private ConcurrentMap<String,HealthWatcher> watchers = new ConcurrentHashMap<>();

    private ScheduledExecutorService executorService;

    private int intervalSeconds = 0;


    public HealthManager() {
        this.executorService = Executors.newSingleThreadScheduledExecutor();
    }


    public void start(){
        if(intervalSeconds > 0){
            this.executorService.scheduleWithFixedDelay(new WatchRunnable(),
                    intervalSeconds,intervalSeconds,TimeUnit.SECONDS);
        }
    }

    public void stop() {
        this.executorService.shutdown();
    }

    public void setIntervalSeconds(int intervalSeconds){
        this.intervalSeconds = 0;
    }

    public void watch(final String host, final HealthWatcher watcher){
        this.watchers.put(host,watcher);
    }


    public void unWatch(final String host){
        this.watchers.remove(host);
    }



    private final class WatchRunnable implements Runnable {

        @Override
        public void run() {
            for(final Map.Entry<String,HealthWatcher> entry : watchers.entrySet()){
                final String host = entry.getKey();
                final HealthWatcher watcher = entry.getValue();
                watcher.health(host,healthCheck(host));
            }
        }
    }


    public static final String OK = "OK";

    public boolean healthCheck(String host){
        String url = host + HttpAPI.VIP_HEALTH;
        BufferedReader in = null;
        try {
            URL realUrl = new URL(url);
            URLConnection connection = realUrl.openConnection();
            connection.setRequestProperty("accept", "*/*");
            connection.setRequestProperty("connection", "Keep-Alive");
            connection.setRequestProperty("user-agent", "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1;SV1)");
            connection.connect();
            in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String line;
            String result = "";
            while ((line = in.readLine()) != null) {
                result += line;
            }
            if(result.isEmpty()){
                return false;
            }
            // 健康检查
            return OK.equals(result.toUpperCase());
        } catch (Exception e) {
            LOG.warn("the host {} maybe not health, so will be remove,{}", host,e);
        } finally {
            try {
                if (in != null) {
                    in.close();
                }
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        }
        return false;
    }
}
