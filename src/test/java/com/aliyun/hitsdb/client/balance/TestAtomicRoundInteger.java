package com.aliyun.hitsdb.client.balance;

import org.junit.Test;

public class TestAtomicRoundInteger {
    
    @Test
    public void get() throws InterruptedException {
        final AtomicRoundInteger i = new AtomicRoundInteger(10);
        
        for(int n = 0;n<10;n++) {
            new Thread(new Runnable() {
                
                @Override
                public void run() {
                    while(true) {
                        System.out.println(i.getAndNext());
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                     }
                }   
            }).start();
        }
        
        Thread.sleep(10000*1000);
    }
}
