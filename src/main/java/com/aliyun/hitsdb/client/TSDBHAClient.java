package com.aliyun.hitsdb.client;

import com.aliyun.hitsdb.client.value.Result;
import com.aliyun.hitsdb.client.value.request.Point;
import com.aliyun.hitsdb.client.value.request.Query;
import com.aliyun.hitsdb.client.value.response.QueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;

/**
 * Created by changrui on 2018/8/6.
 * User can connect two nodes in HA solution.
 * They can use this api to config 2 nodes into one client.
 */
public class TSDBHAClient {
    private static final Logger logger = LoggerFactory.getLogger(TSDBHAClient.class);

    private TSDB tsdbMaster, tsdbSlave;

    public TSDBHAClient(Config master, Config slave) {
        tsdbMaster = TSDBClientFactory.connect(master);
        tsdbSlave = TSDBClientFactory.connect(slave);
    }

    /**
     * Sync put only one point
     */
    public Result putSync(Point point) {
        try {
            return tsdbMaster.putSync(point);
        } catch (Exception e) {
            logger.warn("Write to master node point failed: {}, try slave node", e.getMessage());
        }

        try {
            return tsdbSlave.putSync(point);
        } catch (Exception e) {
            logger.error("Write to slave node point failed: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }

    /**
     * Sync put points
     */
    public Result putSync(Collection<Point> points) {
        try {
            return tsdbMaster.putSync(points);
        } catch (Exception e) {
            logger.warn("Write to master node points failed: {}, try slave node", e.getMessage());
        }

        try {
            return tsdbSlave.putSync(points);
        } catch (Exception e) {
            logger.error("Write to slave node points failed: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }

    /**
     * Query from master first. if failed, then try to query from slave
     */
    public List<QueryResult> query(Query query) {
        try {
            return tsdbMaster.query(query);
        } catch (Exception e) {
            logger.warn("Query from master node failed: {}, try slave node", e.getMessage());
        }

        try {
            return tsdbSlave.query(query);
        } catch (Exception e) {
            logger.error("Query from slave node failed: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }

    /**
     * Query from slave first. if failed, then try to query from master
     */
    public List<QueryResult> querySlave(Query query) {
        try {
            return tsdbSlave.query(query);
        } catch (Exception e) {
            logger.warn("Query from slave node failed: {}, try master node", e.getMessage());
        }

        try {
            return tsdbMaster.query(query);
        } catch (Exception e) {
            logger.error("Query from master node failed: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
