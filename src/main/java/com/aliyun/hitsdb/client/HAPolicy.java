package com.aliyun.hitsdb.client;

import com.aliyun.hitsdb.client.http.HttpClient;
import com.aliyun.hitsdb.client.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.ReentrantLock;

/**
 * @author johnnyzou
 * @date 2020/02/07
 */
public class HAPolicy {
    private static final Logger LOGGER = LoggerFactory.getLogger(HAPolicy.class);

    private Pair<String, Integer> secondaryCluster;
    private RetryRule queryRetryRule = RetryRule.Primary;
    private RetryRule writeRetryRule = RetryRule.Primary;
    private int queryRetryTimes;
    private int writeRetryTimes;
    private int checkCount;
    private int intervalSeconds;
    private int totalRetryTimes;
    private boolean haSwitch;
    private boolean isReady;
    /* primaryClient has switched to secondaryCluster */
    private boolean stateChanged;
    private HttpClient primaryClient;
    private HttpClient secondaryClient;
    ReentrantLock lock = new ReentrantLock();
    /*
     * TODO: support RetryInterval
     */
    private long queryRetryInterval = 0;
    private long writeRetryInterval = 0;

    public enum RetryRule {
        /*
         * Only read primary cluster
         */
        Primary,
        /*
         * Only read secondary cluster
         */
        Secondary,
        /*
         * First read primary cluster then read secondary cluster when failed
         */
        PrimaryPreferred,
        /*
         * First read secondary cluster then read primary cluster when failed
         */
        SecondaryPreferred
    }

    public static Builder addSecondaryCluster(String secondaryHost, int secondaryPort) {
        return new Builder(secondaryHost, secondaryPort);
    }


    public static class Builder {
        private HAPolicy policy;

        public Builder(String secondaryHost, int secondaryPort) {
            policy = new HAPolicy();
            policy.secondaryCluster = new Pair<String, Integer>(secondaryHost, secondaryPort);
        }

        @Deprecated
        public Builder setRetryRule(RetryRule rule) {
            policy.queryRetryRule = rule;
            return this;
        }

        @Deprecated
        public Builder setRetryTimes(int retryTimes) {
            if (retryTimes < 0) {
                throw new IllegalArgumentException("retryTimes must greater or equal than 0");
            }
            policy.queryRetryTimes = retryTimes;
            return this;
        }

        public Builder setQueryRetryRule(RetryRule rule, int retryTimes) {
            policy.queryRetryRule = rule;
            if (retryTimes < 0) {
                throw new IllegalArgumentException("retryTimes must greater or equal than 0");
            }
            policy.queryRetryTimes = retryTimes;
            return this;
        }

        public Builder setWriteRetryRule(RetryRule rule, int retryTimes) {
            policy.writeRetryRule = rule;
            if (retryTimes < 0) {
                throw new IllegalArgumentException("retryTimes must greater or equal than 0");
            }
            policy.writeRetryTimes = retryTimes;
            return this;
        }

        public Builder setFailoverRule(int checkCount, int intervalSeconds) {
            if (checkCount < 1) {
                throw new IllegalArgumentException("checkCount must greater or equal than 1");
            }
            if (intervalSeconds < 1) {
                throw new IllegalArgumentException("intervalSeconds must greater or equal than 1 second");
            }
            policy.haSwitch = true;
            policy.checkCount = checkCount;
            policy.intervalSeconds = intervalSeconds;
            return this;
        }

        public HAPolicy build() {
            return policy;
        }
    }

    public String getSecondaryHost() {
        return secondaryCluster.getKey();
    }

    public int getSecondaryPort() {
        return secondaryCluster.getValue();
    }

    public void setSecondaryCluster(String secondaryHost, int secondaryPort) {
        this.secondaryCluster = new Pair<String, Integer>(secondaryHost, secondaryPort);
    }

    @Deprecated
    public RetryRule getRetryRule() {
        return queryRetryRule;
    }

    public RetryRule getQueryRetryRule() {
        return queryRetryRule;
    }

    public boolean isQueryPrimaryFirstRule() {
        return queryRetryRule.equals(RetryRule.Primary) || queryRetryRule.equals(RetryRule.PrimaryPreferred);
    }

    public int getQueryRetryTimes() {
        return queryRetryTimes;
    }

    // current not used
    public long getQueryRetryInterval() {
        return queryRetryInterval;
    }

    public RetryRule getWriteRetryRule() {
        return writeRetryRule;
    }

    public boolean isWritePrimaryFirstRule() {
        return writeRetryRule.equals(RetryRule.Primary) || writeRetryRule.equals(RetryRule.PrimaryPreferred);
    }

    public int getWriteRetryTimes() {
        return writeRetryTimes;
    }

    // current not used
    public long getWriteRetryInterval() {
        return writeRetryInterval;
    }

    public boolean isHaSwitch() {
        return haSwitch;
    }

    public int getCheckCount() {
        return checkCount;
    }

    public int getIntervalSeconds() {
        return intervalSeconds;
    }

    public void setClusterClient(HttpClient primaryClient, HttpClient secondaryClient) {
        this.primaryClient = primaryClient;
        this.secondaryClient = secondaryClient;
        isReady = true;
    }

    protected void addTotalRetryTimes() {
        totalRetryTimes++;
        if (haSwitch && totalRetryTimes > checkCount) {
            lock.lock();
            try {
                if (totalRetryTimes > checkCount) {
                    HttpClient temp = primaryClient;
                    primaryClient = secondaryClient;
                    secondaryClient = temp;
                    resetTotalRetryTimes();
                    LOGGER.error("failed too much, primaryClient and secondaryClient has changed.");
                    checkStateChanged();
                }
            } finally {
                lock.unlock();
            }
        }
    }

    public void resetClusterClient() {
        if (stateChanged) {
            lock.lock();
            try {
                checkStateChanged();
                if (stateChanged == true) {
                    HttpClient temp = primaryClient;
                    primaryClient = secondaryClient;
                    secondaryClient = temp;
                    LOGGER.info("cluster client has reset, current main cluster : {}:{}", primaryClient.getHost(), primaryClient.getPort());
                    stateChanged = false;
                }
            } finally {
                lock.unlock();
            }
        }
    }

    public void checkStateChanged() {
        if (primaryClient.getHost() == secondaryCluster.getKey() && primaryClient.getPort() == secondaryCluster.getValue()) {
            stateChanged = true;
        } else {
            stateChanged = false;
        }
    }

    protected void resetTotalRetryTimes() {
        totalRetryTimes = 0;
    }

    public HttpClient getPrimaryClient() {
        if (haSwitch && totalRetryTimes > checkCount) {
            return secondaryClient;
        }
        return primaryClient;
    }

    public HttpClient getSecondaryClient() {
        if (haSwitch && totalRetryTimes > checkCount) {
            return primaryClient;
        }
        return secondaryClient;
    }

    public QueryContext creatQueryContext() {
        if (!isReady) {
            throw new IllegalStateException("HA Policy is not ready");
        }
        return new QueryContext(this, getPrimaryClient(), getSecondaryClient());
    }

    public WriteContext creatWriteContext() {
        if (!isReady) {
            throw new IllegalStateException("HA Policy is not ready");
        }
        return new WriteContext(this, getPrimaryClient(), getSecondaryClient());
    }


    public class QueryContext {
        private HAPolicy haPolicy;
        private int retryTimes = 0;
        private HttpClient primaryClient;
        private HttpClient secondaryClient;
        private QueryContext(HAPolicy haPolicy, HttpClient primaryClient, HttpClient secondaryClient) {
            this.haPolicy = haPolicy;
            this.primaryClient = primaryClient;
            this.secondaryClient = secondaryClient;
        }

        public boolean doQuery(){
            if (haPolicy.getQueryRetryRule() == RetryRule.Primary || haPolicy.getQueryRetryRule() == RetryRule.Secondary) {
                if (retryTimes < haPolicy.getQueryRetryTimes()) {
                    return true;
                }
            } else if (haPolicy.getQueryRetryRule() == RetryRule.PrimaryPreferred || haPolicy.getQueryRetryRule() == RetryRule.SecondaryPreferred) {
                if (retryTimes < haPolicy.getQueryRetryTimes() + 1) {
                    // retry on another client
                    return true;
                }
            }
            return false;
        }

        public void addRetryTimes() {
            retryTimes++;
            if (haPolicy.isQueryPrimaryFirstRule()) {
                HAPolicy.this.addTotalRetryTimes();
            }
        }

        public void success(){
            if (haPolicy.isQueryPrimaryFirstRule()) {
                if (retryTimes <= haPolicy.getQueryRetryTimes()) {
                    HAPolicy.this.resetTotalRetryTimes();
                }
            }
        }

        public HttpClient getClient() {
            switch (haPolicy.getQueryRetryRule()) {
                case Primary:
                    return primaryClient;
                case Secondary:
                    return secondaryClient;
                case SecondaryPreferred:
                    if (retryTimes <= haPolicy.getQueryRetryTimes()) {
                        return secondaryClient;
                    }
                    return primaryClient;
                case PrimaryPreferred:
                    if (retryTimes <= haPolicy.getQueryRetryTimes()) {
                        return primaryClient;
                    }
                    return secondaryClient;
                default:
                    throw new IllegalArgumentException("Unknown Retry Policy");
            }
        }
    }


    public class WriteContext {
        private HAPolicy haPolicy;
        private int retryTimes = 0;
        private HttpClient primaryClient;
        private HttpClient secondaryClient;
        private WriteContext(HAPolicy haPolicy, HttpClient primaryClient, HttpClient secondaryClient) {
            this.haPolicy = haPolicy;
            this.primaryClient = primaryClient;
            this.secondaryClient = secondaryClient;
        }

        public boolean doWrite(){
            if (haPolicy.getWriteRetryRule() == RetryRule.Primary || haPolicy.getWriteRetryRule() == RetryRule.Secondary) {
                if (retryTimes < haPolicy.getWriteRetryTimes()) {
                    return true;
                }
            } else if (haPolicy.getWriteRetryRule() == RetryRule.PrimaryPreferred || haPolicy.getWriteRetryRule() == RetryRule.SecondaryPreferred) {
                if (retryTimes < haPolicy.getWriteRetryTimes() + 1) {
                    // retry on another client
                    return true;
                }
            }
            return false;
        }

        public void addRetryTimes() {
            retryTimes++;
            if (haPolicy.isWritePrimaryFirstRule()) {
                HAPolicy.this.addTotalRetryTimes();
            }
        }

        public void success(){
            if (haPolicy.isWritePrimaryFirstRule()) {
                if (retryTimes <= haPolicy.getWriteRetryTimes()) {
                    HAPolicy.this.resetTotalRetryTimes();
                }
            }
        }

        public HttpClient getClient() {
            switch (haPolicy.getWriteRetryRule()) {
                case Primary:
                    return primaryClient;
                case Secondary:
                    return secondaryClient;
                case SecondaryPreferred:
                    if (retryTimes <= haPolicy.getWriteRetryTimes()) {
                        return secondaryClient;
                    }
                    return primaryClient;
                case PrimaryPreferred:
                    if (retryTimes <= haPolicy.getWriteRetryTimes()) {
                        return primaryClient;
                    }
                    return secondaryClient;
                default:
                    throw new IllegalArgumentException("Unknown Retry Policy");
            }
        }
    }
}
