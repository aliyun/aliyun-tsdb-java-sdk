package com.aliyun.hitsdb.client.balance;

import com.aliyun.hitsdb.client.BalHiTSDBClient;
import com.aliyun.hitsdb.client.HiTSDB;
import com.aliyun.hitsdb.client.value.request.Point;

import java.io.File;
import java.io.IOException;

/**
 * Created By jianhong.hjh
 * Date: 2018/10/13
 */
public class TestBalClient {

    public static void main(String[] args) throws IOException {
        HiTSDB client = new BalHiTSDBClient(new File("conf/tsdb.conf"));
        try {
            for(int i = 0;i < 10000;i ++){
                client.putSync(Point.metric("hh.test")
                        .tag("hh","hh")
                        .value(System.currentTimeMillis(),1234)
                        .build());
                Thread.sleep(1000);
            }
        } catch (Exception e){
            e.printStackTrace();
        }

    }
}
