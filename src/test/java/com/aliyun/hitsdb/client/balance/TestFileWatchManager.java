package com.aliyun.hitsdb.client.balance;

import com.aliyun.hitsdb.client.util.FileWatcher;
import com.aliyun.hitsdb.client.util.WatchManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

/**
 * Created By jianhong.hjh
 * Date: 2018/10/13
 */
public class TestFileWatchManager {

    WatchManager watchManager;

    @Before
    public void before() {
        watchManager = new WatchManager();
        watchManager.setIntervalSeconds(2);
        watchManager.start();
    }

    @Test
    public void test() {
        watchManager.watchFile(new File("conf/tsdb.conf"), new FileWatcher() {
            @Override
            public void fileModified(File file) {
                System.out.println(file.getName());
            }
        });
        while (true) {

        }
    }


    @After
    public void after() {
        watchManager.stop();
    }
}
