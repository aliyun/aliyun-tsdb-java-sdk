package com.aliyun.hitsdb.client;

import com.aliyun.hitsdb.client.callback.AbstractBatchPutCallback;
import com.aliyun.hitsdb.client.callback.QueryCallback;
import com.aliyun.hitsdb.client.exception.http.HttpUnknowStatusException;
import com.aliyun.hitsdb.client.http.Host;
import com.aliyun.hitsdb.client.util.*;
import com.aliyun.hitsdb.client.value.Result;
import com.aliyun.hitsdb.client.value.request.*;
import com.aliyun.hitsdb.client.value.response.*;
import com.aliyun.hitsdb.client.value.type.Suggest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * @deprecated
 * @since 0.2.1
 */
public class BalHiTSDBClient extends BalTSDBClient implements HiTSDB {

    public BalHiTSDBClient(File configFile) throws IOException {
        super(configFile);
    }

    public BalHiTSDBClient(String configFilePath) throws IOException {
        super(configFilePath);
    }

    public BalHiTSDBClient(File configFile, AbstractBatchPutCallback<?> callback) throws IOException {
        super(configFile, callback);
    }

    public BalHiTSDBClient(HiTSDBConfig config) {
        super(config);
    }
}