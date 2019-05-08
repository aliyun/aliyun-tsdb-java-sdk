package com.aliyun.hitsdb.client;

/**
 * Created by changrui on 2018/8/6.
 * User can connect two nodes in HA solution.
 * They can use this api to config 2 nodes into one client.
 * @deprecated
 * @since 0.2.1
 */
public class HiTSDBHAClient extends TSDBHAClient {

    public HiTSDBHAClient(HiTSDBConfig master, HiTSDBConfig slave) {
        super(master, slave);
    }
}
