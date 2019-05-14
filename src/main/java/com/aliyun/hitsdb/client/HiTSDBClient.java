package com.aliyun.hitsdb.client;

import com.aliyun.hitsdb.client.exception.http.HttpClientInitException;

/**
 * @since 0.2.1
 * @deprecated use {@link TSDBClient} instead.
 */
@Deprecated
public class HiTSDBClient extends TSDBClient implements HiTSDB {

    public HiTSDBClient(Config config) throws HttpClientInitException {
        super(config);
    }
}