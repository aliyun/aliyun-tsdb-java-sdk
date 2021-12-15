package com.aliyun.hitsdb.client;

import com.aliyun.hitsdb.client.http.HAHttpClient;
import com.aliyun.hitsdb.client.http.HttpClient;
import com.aliyun.hitsdb.client.util.Pair;

/**
 * @author johnnyzou
 * @date 2020/02/07
 */
public class HAPolicy {
    private Pair<String, Integer> secondaryCluster;
    private ReadRule readRule = ReadRule.SecondaryPreferred;
    private WriteRule writeRule = WriteRule.Primary;
    private int queryRetryTimes = 0;
    /*
     * unit: second
     * default as 10 seconds
     */
    private int healthCheckTimeout = 10;
    /*
     * unit: second
     * default as 5 seconds
     */
    private int healthCheckInterval = 5;
    /*
     * TODO: support queryRetryInterval
     */
    private long queryRetryInterval = 0;

    public enum ReadRule {
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
        SecondaryPreferred,
        /*
         * HealthCheck determine which cluster to read
         */
        HealthCheck,
    }

    public enum WriteRule {
        /*
         * Only Write primary
         */
        Primary,
        /*
         * HealthCheck determine which cluster to write
         */
        HealthCheck,
    }

    /**
     * @return write primary and read primary policy , as default policy
     */
    public static HAPolicy createDefaultHAPolicy() {
        HAPolicy haPolicy = new HAPolicy();
        haPolicy.writeRule = WriteRule.Primary;
        haPolicy.readRule = ReadRule.Primary;
        haPolicy.secondaryCluster = null;
        haPolicy.queryRetryTimes = 0;
        haPolicy.checkValid();
        return haPolicy;
    }

    public static Builder addSecondaryCluster(String secondaryHost, int secondaryPort) {
        return new Builder(secondaryHost, secondaryPort);
    }

    private void checkValid() {
        if ( !hasSecondaryCluster()) {
            if (writeShouldHaveSecondaryCluster()) {
                throw new IllegalArgumentException("writeRule should specify secondaryCluster");
            }
            if (readShouldHaveSecondaryCluster()) {
                throw new IllegalArgumentException("readRule should specify secondaryCluster");
            }
        }
    }

    private boolean writeShouldHaveSecondaryCluster() {
        return getWriteRule() != WriteRule.Primary;
    }

    private boolean readShouldHaveSecondaryCluster() {
        return getReadRule() != ReadRule.Primary;
    }

    public boolean hasSecondaryCluster() {
        return this.secondaryCluster != null;
    }

    public boolean hasHealthCheckRule() {
        return getReadRule() == ReadRule.HealthCheck || getWriteRule() == WriteRule.HealthCheck;
    }

    public int getHealthCheckTimeout() {
        return healthCheckTimeout;
    }

    public int getHealthCheckInterval() {
        return healthCheckInterval;
    }

    public static class Builder {
        private HAPolicy policy;

        public Builder(String secondaryHost, int secondaryPort) {
            policy = new HAPolicy();
            policy.secondaryCluster = new Pair<String, Integer>(secondaryHost, secondaryPort);
        }

        public Builder healthCheck() {
            policy.readRule = ReadRule.HealthCheck;
            policy.writeRule = WriteRule.HealthCheck;
            return this;
        }

        public Builder setReadRule(ReadRule rule) {
            policy.readRule = rule;
            return this;
        }

        public Builder setWriteRule(WriteRule rule) {
            policy.writeRule = rule;
            return this;
        }

        public Builder setQueryRetryTimes(int retryTimes) {
            if (retryTimes < 0) {
                throw new IllegalArgumentException("retryTimes must greater or equal than 0");
            }
            policy.queryRetryTimes = retryTimes;
            return this;
        }

        public Builder setHealthCheckTimeout(int timeout) {
            if (timeout <= 0) {
                throw new IllegalArgumentException("healthCheckTimeout must greater than 0");
            }
            policy.healthCheckTimeout = timeout;
            return this;
        }

        public Builder setHealthCheckInterval(int interval) {
            if (interval <= 0) {
                throw new IllegalArgumentException("healthCheckTimeout must greater than 0");
            }
            policy.healthCheckInterval = interval;
            return this;
        }

        public HAPolicy build() {
            policy.checkValid();
            return policy;
        }
    }

    public String getSecondaryHost() {
        return secondaryCluster.getKey();
    }

    public int getSecondaryPort() {
        return secondaryCluster.getValue();
    }

    public ReadRule getReadRule() {
        return readRule;
    }

    public WriteRule getWriteRule() {
        return writeRule;
    }

    public int getQueryRetryTimes() {
        return queryRetryTimes;
    }

    public long getQueryRetryInterval() {
        return queryRetryInterval;
    }

    public static class QueryContext {
        private int retryTimes = 0;

        private HAHttpClient haHttpClient;

        public QueryContext(HAHttpClient haHttpClient) {
            this.retryTimes = 0;
            this.haHttpClient = haHttpClient;
        }

        public boolean doQuery() {
            if (haHttpClient.getHaPolicy().getReadRule() == ReadRule.Primary || haHttpClient.getHaPolicy().getReadRule() == ReadRule.Secondary || haHttpClient.getHaPolicy().getReadRule() == ReadRule.HealthCheck) {
                if (retryTimes < haHttpClient.getHaPolicy().getQueryRetryTimes()) {
                    return true;
                }
            } else if (haHttpClient.getHaPolicy().getReadRule() == ReadRule.PrimaryPreferred || haHttpClient.getHaPolicy().getReadRule() == ReadRule.SecondaryPreferred) {
                if (retryTimes < haHttpClient.getHaPolicy().getQueryRetryTimes() + 1) {
                    // retry on another client
                    return true;
                }
            }
            return false;
        }

        public void addRetryTimes() {
            retryTimes++;
        }

        public HttpClient getClient() {
            return this.haHttpClient.getReadClient(retryTimes);
        }

    }

}
