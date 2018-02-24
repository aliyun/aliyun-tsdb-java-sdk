package com.aliyun.hitsdb.client.balance;

import java.util.concurrent.locks.ReentrantLock;

public class AtomicRoundInteger {
    private int num;
    private int size;
    private ReentrantLock lock = new ReentrantLock();

    public AtomicRoundInteger(int size) {
        this.size = size;
    }

    public int getAndNext() {
        try {
            lock.lock();
            int i = num;
            num++;
            if(num == size) {
                num = 0;
            }
            return i;
        } finally {
            lock.unlock();
        }
    }
    
}
