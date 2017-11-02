package com.alibaba.hitsdb.client.consumer;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

class BatchPutThreadFactory implements ThreadFactory {
    private static final AtomicInteger poolNumber = new AtomicInteger(1);
    private ThreadGroup group;
    private String namePrefix;
    private AtomicInteger threadNumber;
    
    public BatchPutThreadFactory() {
        threadNumber = new AtomicInteger(1);
        SecurityManager s = System.getSecurityManager();
        group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
        namePrefix = "pool-" + poolNumber.getAndIncrement() + "-batch-put-thread-";
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread thread = new Thread(group, r, namePrefix + threadNumber.getAndIncrement(), 0);
        if (thread.isDaemon()) {
            // 作为守护线程存在
            thread.setDaemon(true);
        }
        
        if (thread.getPriority() != Thread.NORM_PRIORITY) {
            thread.setPriority(Thread.NORM_PRIORITY);
        }
        
        return thread;
    }

}