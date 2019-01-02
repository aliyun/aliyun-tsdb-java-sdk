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
 */
public class BalHiTSDBClient implements HiTSDB {

    private static final Logger LOG = LoggerFactory.getLogger(BalHiTSDBClient.class);

    private HiTSDBConfig config;

    private volatile Map<String, HiTSDB> healthClientMap = new ConcurrentHashMap();
    private volatile Map<String, HiTSDB> nonHealthClientMap = new ConcurrentHashMap();
    private volatile List<HiTSDB> clients = new ArrayList();

    private AtomicBoolean SYNC = new AtomicBoolean(true);

    private WatchManager watchManager;
    private HealthManager healthManager;

    private volatile int idx = 0;

    /**
     * 默认每2秒监听文件是否发生变化
     */
    private static final int DEFAULT_WATCH_FILE_INTERVALS = 2;

    public BalHiTSDBClient(File configFile) throws IOException {
        this(configFile, null);
    }


    public BalHiTSDBClient(String configFilePath) throws IOException {
        this(new File(configFilePath));
    }


    public BalHiTSDBClient(File configFile, AbstractBatchPutCallback<?> callback) throws IOException {
        this.initFileWatchManager(configFile);
        //
        HiTSDBConfig config = loadConfig(configFile);
        if (callback != null) {
            config.setBatchPutCallback(callback);
        }
        this.init(config);
    }


    private HiTSDBConfig loadConfig(File configFile) throws IOException {
        // 读取配置文件的配置项
        Properties properties = new Properties();
        properties.load(new FileReader(configFile));
        PropKit propKit = new PropKit(properties);

        HiTSDBConfig.Builder builder = HiTSDBConfig.builder();
        if (propKit.containsKey("host")) {
            if (propKit.containsKey("port")) {
                builder.addAddress(propKit.get("host").trim(), propKit.getInt("port"));
            } else {
                builder.addAddress(propKit.get("host").trim());
            }
        } else if (propKit.containsKey("address")) {
            //
            String address = propKit.get("address");
            String[] hosts = address.split(",");
            if (hosts.length > 0) {
                for (String hostString : hosts) {
                    String[] hh = hostString.split(":");
                    if (hh.length == 1) {
                        builder.addAddress(hh[0].trim());
                    } else {
                        builder.addAddress(hh[0].trim(), Integer.parseInt(hh[1]));
                    }
                }
            } else {
                throw new IllegalStateException("the address must not be empty");
            }
        } else {
            throw new IllegalStateException("Specify at least one tsdb address");
        }

        if (propKit.containsKey("batchPutSize")) {
            builder.batchPutSize(propKit.getInt("batchPutSize"));
        }
        if (propKit.containsKey("batchPutTimeLimit")) {
            builder.batchPutTimeLimit(propKit.getInt("batchPutTimeLimit"));
        }
        if (propKit.containsKey("batchPutBufferSize")) {
            builder.batchPutBufferSize(propKit.getInt("batchPutBufferSize"));
        }
        if (propKit.containsKey("batchPutRetryCount")) {
            builder.batchPutRetryCount(propKit.getInt("batchPutRetryCount"));
        }
        if (propKit.containsKey("httpConnectionPool")) {
            builder.httpConnectionPool(propKit.getInt("httpConnectionPool"));
        }
        if (propKit.containsKey("httpConnectTimeout")) {
            builder.httpConnectTimeout(propKit.getInt("httpConnectTimeout"));
        }
        if (propKit.containsKey("putRequestLimit")) {
            builder.putRequestLimit(propKit.getInt("putRequestLimit"));
        }
        if (propKit.containsKey("batchPutConsumerThreadCount")) {
            builder.batchPutConsumerThreadCount(propKit.getInt("batchPutConsumerThreadCount"));
        }
        if (propKit.containsKey("httpCompress")) {
            builder.httpCompress(propKit.getBoolean("httpCompress"));
        }
        if (propKit.containsKey("ioThreadCount")) {
            builder.ioThreadCount(propKit.getInt("ioThreadCount"));
        }
        if (propKit.containsKey("backpressure")) {
            builder.backpressure(propKit.getBoolean("backpressure"));
        }
        if (propKit.containsKey("httpConnectionLiveTime")) {
            builder.httpConnectionLiveTime(propKit.getInt("httpConnectionLiveTime"));
        }
        if (propKit.containsKey("httpKeepaliveTime")) {
            builder.httpKeepaliveTime(propKit.getInt("httpKeepaliveTime"));
        }
        if (propKit.containsKey("maxTPS")) {
            builder.maxTPS(propKit.getInt("maxTPS"));
        }
        if (propKit.containsKey("asyncPut")) {
            builder.asyncPut(propKit.getBoolean("asyncPut"));
        }
        return builder.config();
    }

    private void initFileWatchManager(File configFile) {
        this.watchManager = new WatchManager();
        this.watchManager.setIntervalSeconds(DEFAULT_WATCH_FILE_INTERVALS);
        this.watchManager.watchFile(configFile, new FileWatcher() {
            @Override
            public void fileModified(File file) {
                LOG.info("the file {} is changed", file.getAbsolutePath());
                try {
                    doHandle(file);
                } catch (Exception e) {
                    LOG.error("Do handle changed file error", e);
                }
            }
        });
        this.watchManager.start();
    }


    private void doHandle(File file){
        LOG.info("the config file {} has been modified, so reload it", file.getName());
        HiTSDBConfig newConfig = null;
        try {
            // 新的配置项
            newConfig = loadConfig(file);
        } catch (IOException e) {
            LOG.error("Load config file error", e);
        }
        if (newConfig == null) {
            return;
        }
        // 新的客户端
        Map<String, HiTSDB> tHealthMap = new ConcurrentHashMap();
        Map<String, HiTSDB> tNonHealthMap = new ConcurrentHashMap();
        List<HiTSDB> tClients = new ArrayList();

        Map<String, HiTSDB> thHolder = healthClientMap;
        Map<String, HiTSDB> tnhHolder = nonHealthClientMap;
        List<HiTSDB> tcHolder = clients;
        HiTSDBConfig tConfigHolder = config;
        List<String> wathers = new ArrayList();
        SYNC.set(false);
        try {
            wathers.clear();
            // 停止健康检查
            for (String host : healthClientMap.keySet()) {
                healthManager.unWatch(host);
            }
            for (String host : nonHealthClientMap.keySet()) {
                healthManager.unWatch(host);
            }
            //
            config = newConfig;
            nonHealthClientMap = tNonHealthMap;
            healthClientMap = tHealthMap;
            clients = tClients;
            // 创建实例
            wathers = initInstance(config);
            //
            LOG.info("success load config and replace client instance");
            try {
                // 关闭原先的clients
                for (HiTSDB tsdb : thHolder.values()) {
                    tsdb.close();
                }
                for (HiTSDB tsdb : tnhHolder.values()) {
                    tsdb.close();
                }
                tcHolder.clear();
            } catch (Exception e) {
                LOG.error("closed the origin tsdb client instance error", e);
            }
        } catch (Exception e) {
            wathers.clear();
            try {
                // 清理这次失败的添加
                for (HiTSDB tsdb : healthClientMap.values()) {
                    tsdb.close();
                }
                for (HiTSDB tsdb : nonHealthClientMap.values()) {
                    tsdb.close();
                }
            } catch (IOException ex){
                LOG.error("closed the new tsdb client instance error", ex);
            }
            clients.clear();
            // rollback
            LOG.info("An error occurred, so the original configuration is maintained");
            healthClientMap = thHolder;
            nonHealthClientMap = tnhHolder;
            clients = tcHolder;
            config = tConfigHolder;
            // 添加监听项
            wathers.addAll(healthClientMap.keySet());
            wathers.addAll(nonHealthClientMap.keySet());
        } finally {
            SYNC.set(true);
        }
        // 添加健康检查监听
        for (String host : wathers) {
            healthManager.watch(host, healthWatcher);
        }
    }

    private void initHealthWatchManager() {
        this.healthManager = new HealthManager();
        this.healthManager.setIntervalSeconds(DEFAULT_WATCH_FILE_INTERVALS);
        this.healthManager.start();
    }

    public BalHiTSDBClient(HiTSDBConfig config) {
        this.init(config);
    }

    private void init(HiTSDBConfig config) {
        this.config = config;
        this.initHealthWatchManager();
        List<String> wathers = this.initInstance(config);
        // 添加健康检测
        for (String host : wathers) {
            this.healthManager.watch(host, healthWatcher);
        }
    }

    public List<String> initInstance(HiTSDBConfig config) {
        List<Host> hosts = config.getAddresses();
        List<String> wathers = new ArrayList();
        if (!hosts.isEmpty()) {
            // 实例化多个client
            for (Host host : hosts) {
                HiTSDBConfig newConfig = config.copy(host.getIp(), host.getPort());
                wathers.add(initClient(newConfig));
            }
        } else if (config.getHost() != null && !config.getHost().isEmpty()) {
            wathers.add(initClient(config));
        } else {
            throw new IllegalStateException("Specify at least one tsdb address, but there is zero");
        }
        return wathers;
    }

    private final HealthWatcher healthWatcher = new HealthWatcher() {
        @Override
        public void health(String host, boolean health) {
            // 健康则直接返回
            if (health) {
                // 判断这个host 是否在健康列表中，若在这直接返回
                if (healthClientMap.containsKey(host)) {
                    LOG.info("the tsdb is work well : {}", host);
                    return;
                } else {
                    // 不健康的host已经正常了，则添加进正常client列表中
                    HiTSDB client = nonHealthClientMap.remove(host);
                    healthClientMap.put(host, client);
                    List<HiTSDB> list = new ArrayList();
                    list.addAll(healthClientMap.values());
                    SYNC.set(false);
                    try {
                        List<HiTSDB> temp = clients;
                        clients = list;
                        temp.clear();
                    } finally {
                        SYNC.set(true);
                    }
                }
            } else {
                SYNC.set(false);
                try {
                    LOG.info("the host: {} may be not health, so remove it", host);
                    // 正常的host已经不正常了，则添加进不正常列表
                    HiTSDB client = healthClientMap.remove(host);
                    nonHealthClientMap.put(host, client);
                    List<HiTSDB> list = new ArrayList();
                    list.addAll(healthClientMap.values());
                    // 替换可用client列表
                    clients = list;
                } finally {
                    SYNC.set(true);
                }
            }
        }
    };

    private String initClient(HiTSDBConfig config) {
        HiTSDB client = HiTSDBClientFactory.connect(config);
        String host = config.getHost() + ":" + config.getPort();
        SYNC.set(false);
        try {
            this.healthClientMap.put(host, client);
            this.clients.add(client);
        } finally {
            SYNC.set(true);
        }
        return host;
    }

    private HiTSDB client() {
        // 等待更新结束
        while (!SYNC.get());
        if (clients.isEmpty()) {
            throw new RuntimeException("The number of available clients is zero, please check it");
        }
        int index = idx;

        idx = (++idx) % clients.size();
        if (index >= clients.size()) {
            index = 0;
        }
        HiTSDB client = clients.get(index);
        if (client == null) {
            throw new RuntimeException("The client is null");
        }
        return client;
    }


    @Override
    public void put(Point point) {
        Exception exception = null;
        for (int i = 0; i < MAX_RETRY_SIZE; i++) {
            try {
                client().put(point);
                return;
            } catch (Exception e) {
                exception = e;
            }
        }
        throw new RuntimeException(exception);
    }

    @Override
    public void put(Point... points) {
        Exception exception = null;
        for (int i = 0; i < MAX_RETRY_SIZE; i++) {
            try {
                client().put(points);
                return;
            } catch (Exception e) {
                exception = e;
            }
        }
        throw new RuntimeException(exception);
    }

    @Override
    public void multiValuedPut(MultiValuedPoint point) {
        Exception exception = null;
        for (int i = 0; i < MAX_RETRY_SIZE; i++) {
            try {
                client().multiValuedPut(point);
                return;
            } catch (Exception e) {
                exception = e;
            }
        }
        throw new RuntimeException(exception);
    }

    @Override
    public void multiValuedPut(MultiValuedPoint... points) {
        Exception exception = null;
        for (int i = 0; i < MAX_RETRY_SIZE; i++) {
            try {
                client().multiValuedPut(points);
                return;
            } catch (Exception e) {
                exception = e;
            }
        }
        throw new RuntimeException(exception);
    }

    private static int MAX_RETRY_SIZE = 3;

    @Override
    public Result putSync(Collection<Point> points) {
        Exception exception = null;
        for (int i = 0; i < MAX_RETRY_SIZE; i++) {
            try {
                return client().putSync(points);
            } catch (Exception e) {
                for(Point point : points){

                }
                exception = e;
            }
        }
        throw new RuntimeException(exception);
    }

    @Override
    public Result putSync(Point... points) {
        Exception exception = null;
        for (int i = 0; i < MAX_RETRY_SIZE; i++) {
            try {
                return client().putSync(points);
            } catch (Exception e) {
                exception = e;
            }
        }
        throw new RuntimeException(exception);
    }

    @Override
    public <T extends Result> T putSync(Collection<Point> points, Class<T> resultType) {
        Exception exception = null;
        for (int i = 0; i < MAX_RETRY_SIZE; i++) {
            try {
                return client().putSync(points, resultType);
            } catch (Exception e) {
                exception = e;
            }
        }
        throw new RuntimeException(exception);
    }

    @Override
    public <T extends Result> T putSync(Class<T> resultType, Collection<Point> points) {
        Exception exception = null;
        for (int i = 0; i < MAX_RETRY_SIZE; i++) {
            try {
                return client().putSync(resultType, points);
            } catch (Exception e) {
                exception = e;
            }
        }
        throw new RuntimeException(exception);
    }

    @Override
    public <T extends Result> T putSync(Class<T> resultType, Point... points) {
        Exception exception = null;
        for (int i = 0; i < MAX_RETRY_SIZE; i++) {
            try {
                return client().putSync(resultType, points);
            } catch (Exception e) {
                exception = e;
            }
        }
        throw new RuntimeException(exception);
    }

    @Override
    public Result multiValuedPutSync(Collection<MultiValuedPoint> points) {
        Exception exception = null;
        for (int i = 0; i < MAX_RETRY_SIZE; i++) {
            try {
                return client().multiValuedPutSync(points);
            } catch (Exception e) {
                exception = e;
            }
        }
        throw new RuntimeException(exception);
    }

    @Override
    public Result multiValuedPutSync(MultiValuedPoint... points) {
        Exception exception = null;
        for (int i = 0; i < MAX_RETRY_SIZE; i++) {
            try {
                return client().multiValuedPutSync(points);
            } catch (Exception e) {
                exception = e;
            }
        }
        throw new RuntimeException(exception);
    }

    @Override
    public <T extends Result> T multiValuedPutSync(Collection<MultiValuedPoint> points, Class<T> resultType) {
        Exception exception = null;
        for (int i = 0; i < MAX_RETRY_SIZE; i++) {
            try {
                return client().multiValuedPutSync(points, resultType);
            } catch (Exception e) {
                exception = e;
            }
        }
        throw new RuntimeException(exception);
    }

    @Override
    public <T extends Result> T multiValuedPutSync(Class<T> resultType, Collection<MultiValuedPoint> points) {
        Exception exception = null;
        for (int i = 0; i < MAX_RETRY_SIZE; i++) {
            try {
                return client().multiValuedPutSync(resultType, points);
            } catch (Exception e) {
                exception = e;
            }
        }
        throw new RuntimeException(exception);
    }

    @Override
    public <T extends Result> T multiValuedPutSync(Class<T> resultType, MultiValuedPoint... points) {
        Exception exception = null;
        for (int i = 0; i < MAX_RETRY_SIZE; i++) {
            try {
                return client().multiValuedPutSync(resultType, points);
            } catch (Exception e) {
                exception = e;
            }
        }
        throw new RuntimeException(exception);
    }

    @Override
    public void query(Query query, QueryCallback callback) {
        Exception exception = null;
        for (int i = 0; i < MAX_RETRY_SIZE; i++) {
            try {
                client().query(query, callback);
                return;
            } catch (Exception e) {
                exception = e;
            }
        }
        throw new RuntimeException(exception);
    }

    @Override
    public List<QueryResult> query(Query query) throws HttpUnknowStatusException {
        Exception exception = null;
        for (int i = 0; i < MAX_RETRY_SIZE; i++) {
            try {
                return client().query(query);
            } catch (Exception e) {
                exception = e;
            }
        }
        throw new RuntimeException(exception);
    }

    @Override
    public MultiValuedQueryResult multiValuedQuery(MultiValuedQuery query) throws HttpUnknowStatusException {
        Exception exception = null;
        for (int i = 0; i < MAX_RETRY_SIZE; i++) {
            try {
                return client().multiValuedQuery(query);
            } catch (Exception e) {
                exception = e;
            }
        }
        throw new RuntimeException(exception);
    }

    @Override
    public List<QueryResult> last(Query query, int num) throws HttpUnknowStatusException {
        Exception exception = null;
        for (int i = 0; i < MAX_RETRY_SIZE; i++) {
            try {
                return client().last(query, num);
            } catch (Exception e) {
                exception = e;
            }
        }
        throw new RuntimeException(exception);
    }

    @Override
    public void delete(Query query) throws HttpUnknowStatusException {
        Exception exception = null;
        for (int i = 0; i < MAX_RETRY_SIZE; i++) {
            try {
                client().delete(query);
                return;
            } catch (Exception e) {
                exception = e;
            }
        }
        throw new RuntimeException(exception);
    }

    @Override
    public void deleteData(String metric, long startTime, long endTime) throws HttpUnknowStatusException {
        Exception exception = null;
        for (int i = 0; i < MAX_RETRY_SIZE; i++) {
            try {
                client().deleteData(metric, startTime, endTime);
                return;
            } catch (Exception e) {
                exception = e;
            }
        }
        throw new RuntimeException(exception);
    }

    @Override
    public void deleteData(String metric, List<String> fields, long startTime, long endTime) throws HttpUnknowStatusException {
        Exception exception = null;
        for (int i = 0; i < MAX_RETRY_SIZE; i++) {
            try {
                client().deleteData(metric, fields, startTime, endTime);
                return;
            } catch (Exception e) {
                exception = e;
            }
        }
        throw new RuntimeException(exception);
    }

    @Override
    public void deleteData(String metric, Date startDate, Date endDate) throws HttpUnknowStatusException {
        Exception exception = null;
        for (int i = 0; i < MAX_RETRY_SIZE; i++) {
            try {
                client().deleteData(metric, startDate, endDate);
                return;
            } catch (Exception e) {
                exception = e;
            }
        }
        throw new RuntimeException(exception);
    }

    @Override
    public void deleteData(String metric, List<String> fields, Date startDate, Date endDate) throws HttpUnknowStatusException {
        Exception exception = null;
        for (int i = 0; i < MAX_RETRY_SIZE; i++) {
            try {
                client().deleteData(metric, fields, startDate, endDate);
                return;
            } catch (Exception e) {
                exception = e;
            }
        }
        throw new RuntimeException(exception);
    }

    @Override
    public void deleteMeta(String metric, Map<String, String> tags) throws HttpUnknowStatusException {
        Exception exception = null;
        for (int i = 0; i < MAX_RETRY_SIZE; i++) {
            try {
                client().deleteMeta(metric, tags);
                return;
            } catch (Exception e) {
                exception = e;
            }
        }
        throw new RuntimeException(exception);
    }

    @Override
    public void deleteMeta(String metric, List<String> fields, Map<String, String> tags) throws HttpUnknowStatusException {
        Exception exception = null;
        for (int i = 0; i < MAX_RETRY_SIZE; i++) {
            try {
                client().deleteMeta(metric, fields, tags);
                return;
            } catch (Exception e) {
                exception = e;
            }
        }
        throw new RuntimeException(exception);
    }

    @Override
    public void deleteMeta(Timeline timeline) throws HttpUnknowStatusException {
        Exception exception = null;
        for (int i = 0; i < MAX_RETRY_SIZE; i++) {
            try {
                client().deleteMeta(timeline);
                return;
            } catch (Exception e) {
                exception = e;
            }
        }
        throw new RuntimeException(exception);
    }

    @Override
    public void ttl(int lifetime) throws HttpUnknowStatusException {

        Exception exception = null;
        for (int i = 0; i < MAX_RETRY_SIZE; i++) {
            try {
                client().ttl(lifetime);
                return;
            } catch (Exception e) {
                exception = e;
            }
        }
        throw new RuntimeException(exception);
    }

    @Override
    public void ttl(int lifetime, TimeUnit unit) throws HttpUnknowStatusException {
        Exception exception = null;
        for (int i = 0; i < MAX_RETRY_SIZE; i++) {
            try {
                client().ttl(lifetime, unit);
                return;
            } catch (Exception e) {
                exception = e;
            }
        }
        throw new RuntimeException(exception);
    }

    @Override
    public int ttl() throws HttpUnknowStatusException {

        Exception exception = null;
        for (int i = 0; i < MAX_RETRY_SIZE; i++) {
            try {
                return client().ttl();
            } catch (Exception e) {
                exception = e;
            }
        }
        throw new RuntimeException(exception);
    }

    @Override
    public List<String> suggest(Suggest type, String prefix, int max) throws HttpUnknowStatusException {
        Exception exception = null;
        for (int i = 0; i < MAX_RETRY_SIZE; i++) {
            try {
                return client().suggest(type, prefix, max);
            } catch (Exception e) {
                exception = e;
            }
        }
        throw new RuntimeException(exception);
    }

    @Override
    public List<String> suggest(Suggest type, String metric, String prefix, int max) throws HttpUnknowStatusException {
        Exception exception = null;
        for (int i = 0; i < MAX_RETRY_SIZE; i++) {
            try {
                return client().suggest(type, metric, prefix, max);
            } catch (Exception e) {
                exception = e;
            }
        }
        throw new RuntimeException(exception);
    }

    @Override
    public List<LookupResult> lookup(String metric, List<LookupTagFilter> tags, int max) throws HttpUnknowStatusException {
        Exception exception = null;
        for (int i = 0; i < MAX_RETRY_SIZE; i++) {
            try {
                return client().lookup(metric, tags, max);
            } catch (Exception e) {
                exception = e;
            }
        }
        throw new RuntimeException(exception);
    }

    @Override
    public List<LookupResult> lookup(LookupRequest lookupRequest) throws HttpUnknowStatusException {
        Exception exception = null;
        for (int i = 0; i < MAX_RETRY_SIZE; i++) {
            try {
                return client().lookup(lookupRequest);
            } catch (Exception e) {
                exception = e;
            }
        }
        throw new RuntimeException(exception);
    }

    @Override
    public List<TagResult> dumpMeta(String tagkey, String tagValuePrefix, int max) throws HttpUnknowStatusException {
        Exception exception = null;
        for (int i = 0; i < MAX_RETRY_SIZE; i++) {
            try {
                return client().dumpMeta(tagkey, tagValuePrefix, max);
            } catch (Exception e) {
                exception = e;
            }
        }
        throw new RuntimeException(exception);
    }

    @Override
    public List<TagResult> dumpMeta(String metric, String tagkey, String tagValuePrefix, int max) throws HttpUnknowStatusException {
        Exception exception = null;
        for (int i = 0; i < MAX_RETRY_SIZE; i++) {
            try {
                return client().dumpMeta(metric, tagkey, tagValuePrefix, max);
            } catch (Exception e) {
                exception = e;
            }
        }
        throw new RuntimeException(exception);
    }

    @Override
    public List<String> dumpMetric(String tagkey, String tagValuePrefix, int max) throws HttpUnknowStatusException {
        Exception exception = null;
        for (int i = 0; i < MAX_RETRY_SIZE; i++) {
            try {
                return client().dumpMetric(tagkey, tagValuePrefix, max);
            } catch (Exception e) {
                exception = e;
            }
        }
        throw new RuntimeException(exception);
    }


    @Override
    public MultiValuedQueryLastResult multiValuedQueryLast(MultiValuedQueryLastRequest queryLastRequest) throws HttpUnknowStatusException {
        Exception exception = null;
        for (int i = 0; i < MAX_RETRY_SIZE; i++) {
            try {
                return client().multiValuedQueryLast(queryLastRequest);
            } catch (Exception e) {
                exception = e;
            }
        }
        throw new RuntimeException(exception);
    }

    @Override
    public List<LastDataValue> queryLast(Collection<Timeline> timelines) throws HttpUnknowStatusException {
        Exception exception = null;
        for (int i = 0; i < MAX_RETRY_SIZE; i++) {
            try {
                return client().queryLast(timelines);
            } catch (Exception e) {
                exception = e;
            }
        }
        throw new RuntimeException(exception);
    }

    @Override
    public List<LastDataValue> queryLast(Timeline... timelines) throws HttpUnknowStatusException {
        Exception exception = null;
        for (int i = 0; i < MAX_RETRY_SIZE; i++) {
            try {
                return client().queryLast(timelines);
            } catch (Exception e) {
                exception = e;
            }
        }
        throw new RuntimeException(exception);
    }

    @Override
    public List<LastDataValue> queryLast(List<String> tsuids) throws HttpUnknowStatusException {
        Exception exception = null;
        for (int i = 0; i < MAX_RETRY_SIZE; i++) {
            try {
                return client().queryLast(tsuids);
            } catch (Exception e) {
                exception = e;
            }
        }
        throw new RuntimeException(exception);
    }

    @Override
    public List<LastDataValue> queryLast(LastPointQuery query) throws HttpUnknowStatusException {
        Exception exception = null;
        for (int i = 0; i < MAX_RETRY_SIZE; i++) {
            try {
                return client().queryLast(query);
            } catch (Exception e) {
                exception = e;
            }
        }
        throw new RuntimeException(exception);
    }

    @Override
    public boolean truncate() throws HttpUnknowStatusException {
       return client().truncate();
    }

    @Override
    public List<LastDataValue> queryLast(String... tsuids) throws HttpUnknowStatusException {
        Exception exception = null;
        for (int i = 0; i < MAX_RETRY_SIZE; i++) {
            try {
                return client().queryLast(tsuids);
            } catch (Exception e) {
                exception = e;
            }
        }
        throw new RuntimeException(exception);
    }

    /**
     * Following APIs, which have multiField prefix, are for TSDB's multi-field data model structure's puts and queries.
     * Since TSDB release 2.3.7
     */
    @Override
    public <T extends Result> T multiFieldPutSync(Collection<MultiFieldPoint> points, Class<T> resultType) {
        Exception exception = null;
        for (int i = 0; i < MAX_RETRY_SIZE; i++) {
            try {
                return client().multiFieldPutSync(points, resultType);
            } catch (Exception e) {
                exception = e;
            }
        }
        throw new RuntimeException(exception);
    }

    @Override
    public Result multiFieldPutSync(MultiFieldPoint... points) {
        Exception exception = null;
        for (int i = 0; i < MAX_RETRY_SIZE; i++) {
            try {
                return client().multiFieldPutSync(points);
            } catch (Exception e) {
                exception = e;
            }
        }
        throw new RuntimeException(exception);
    }

    @Override
    public Result multiFieldPutSync(Collection<MultiFieldPoint> points) {
        Exception exception = null;
        for (int i = 0; i < MAX_RETRY_SIZE; i++) {
            try {
                return client().multiFieldPutSync(points);
            } catch (Exception e) {
                exception = e;
            }
        }
        throw new RuntimeException(exception);
    }

    @Override
    public List<MultiFieldQueryResult> multiFieldQuery(MultiFieldQuery query) throws HttpUnknowStatusException {
        Exception exception = null;
        for (int i = 0; i < MAX_RETRY_SIZE; i++) {
            try {
                return client().multiFieldQuery(query);
            } catch (Exception e) {
                exception = e;
            }
        }
        throw new RuntimeException(exception);
    }

    @Override
    public List<MultiFieldQueryLastResult> multiFieldQueryLast(LastPointQuery lastPointQuery) throws HttpUnknowStatusException {
        Exception exception = null;
        for (int i = 0; i < MAX_RETRY_SIZE; i++) {
            try {
                return client().multiFieldQueryLast(lastPointQuery);
            } catch (Exception e) {
                exception = e;
            }
        }
        throw new RuntimeException(exception);
    }

    @Override
    public String version() throws HttpUnknowStatusException {
        return client().version();
    }

    @Override
    public Map<String, String> getVersionInfo() throws HttpUnknowStatusException {
        return client().getVersionInfo();
    }

    @Override
    public void close() throws IOException {
        close(false);
    }

    @Override
    public void close(boolean force) throws IOException {
        SYNC.set(false);
        try {
            for (HiTSDB client : this.healthClientMap.values()) {
                client.close(force);
            }
            for (HiTSDB client : this.nonHealthClientMap.values()) {
                client.close(force);
            }
            this.clients.clear();
            if (this.watchManager != null) {
                this.watchManager.stop();
            }
            if (this.healthManager != null) {
                this.healthManager.stop();
            }
        } finally {
            SYNC.set(true);
        }
    }
}
