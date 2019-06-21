package com.aliyun.hitsdb.client;

import com.aliyun.hitsdb.client.exception.NotImplementedException;
import com.aliyun.hitsdb.client.exception.http.HttpClientInitException;
import com.aliyun.hitsdb.client.exception.http.HttpUnknowStatusException;
import com.aliyun.hitsdb.client.value.response.SQLResult;

/**
 * @since 0.2.1
 * @deprecated use {@link TSDBClient} instead.
 */
@Deprecated
public class HiTSDBClient extends TSDBClient implements HiTSDB {

    public HiTSDBClient(Config config) throws HttpClientInitException {
        super(config);
    }

    @Override
    public SQLResult queryBySQL(String sql) throws HttpUnknowStatusException {
        throw new NotImplementedException();
    }
}
