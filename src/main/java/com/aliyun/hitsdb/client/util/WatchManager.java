package com.aliyun.hitsdb.client.util;

import java.io.File;
import java.util.Map;
import java.util.concurrent.*;

public class WatchManager {

    private ConcurrentMap<File,FileMonitor> watchers = new ConcurrentHashMap<>();

    private ScheduledExecutorService executorService;

    private int intervalSeconds = 0;


    public WatchManager() {
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

    public void watchFile(final File file,final FileWatcher watcher){
        final long lastModified = file.lastModified();
        watchers.put(file,new FileMonitor(lastModified,watcher));
    }



    private final class WatchRunnable implements Runnable {

        @Override
        public void run() {
            for(final Map.Entry<File,FileMonitor> entry : watchers.entrySet()){
                final File file = entry.getKey();
                final FileMonitor fileMonitor = entry.getValue();
                final long lastModified = file.lastModified();
                if(fileModified(fileMonitor,lastModified)){
                    fileMonitor.lastModifiedMillis = lastModified;
                    fileMonitor.fileWatcher.fileModified(file);
                }
            }
        }

        private boolean fileModified(final FileMonitor fileMonitor, final long lastModifiedMillis){
            return lastModifiedMillis != fileMonitor.lastModifiedMillis;
        }
    }


    private final class FileMonitor {
        private final FileWatcher fileWatcher;
        private volatile long lastModifiedMillis;

        public FileMonitor(final long lastModifiedMillis,final FileWatcher fileWatcher){
            this.fileWatcher = fileWatcher;
            this.lastModifiedMillis = lastModifiedMillis;
        }

        private void setLastModifiedMillis(final long lastModifiedMillis){
            this.lastModifiedMillis = lastModifiedMillis;
        }

        @Override
        public String toString() {
            return "FileMonitor [fileWatcher=" + fileWatcher + ", lastModifiedMillis=" + lastModifiedMillis + "]";
        }
    }
}
