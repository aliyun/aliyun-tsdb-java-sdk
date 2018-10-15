package com.aliyun.hitsdb.client.balance;

import com.aliyun.hitsdb.client.util.HealthManager;
import com.aliyun.hitsdb.client.util.HealthWatcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Created By jianhong.hjh
 * Date: 2018/10/13
 */
public class TestHealthManager {

    HealthManager healthManager;



    @Before
    public void before() {
        healthManager = new HealthManager();
        healthManager.setIntervalSeconds(2);
        healthManager.start();
    }


    @Test
    public void test() {
        String[] address = "127.0.0.1:8242".split(",");
        for (String host : address) {
            healthManager.watch(host, new HealthWatcher() {
                @Override
                public void health(String host, boolean health) {
                    System.out.println(host + "\t" + health);
                }
            });
        }
        while (true){

        }
    }





    @After
    public void after(){
        healthManager.stop();
    }
}
