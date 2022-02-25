package com.aliyun.hitsdb.client;

import com.aliyun.hitsdb.client.http.HttpClient;
import com.aliyun.hitsdb.client.util.Pair;

/**
 * @author johnnyzou
 * @date 2020/02/07
 */
public class HAPolicy {
    private Pair<String, Integer> secondaryCluster;
    private RetryRule retryRule = RetryRule.SecondaryPreferred;
    private int queryRetryTimes;
    private boolean haSwitch;
    private int checkCount;
    private int intervalSeconds;
    /*
     * TODO: support queryRetryInterval
     */
    private long queryRetryInterval = 0;

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

        public Builder setRetryRule(RetryRule rule) {
            policy.retryRule = rule;
            return this;
        }

        public Builder setRetryTimes(int retryTimes) {
            if (retryTimes < 0) {
                throw new IllegalArgumentException("retryTimes must greater or equal than 0");
            }
            policy.queryRetryTimes = retryTimes;
            return this;
        }

        public Builder setHASwitch(int checkCount, int intervalSeconds) {
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

    public RetryRule getRetryRule() {
        return retryRule;
    }

    public int getQueryRetryTimes() {
        return queryRetryTimes;
    }

    public long getQueryRetryInterval() {
        return queryRetryInterval;
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

    public static class QueryContext {
        private HAPolicy haPolicy;
        private int retryTimes = 0;
        private HttpClient primaryClient;
        private HttpClient secondaryClient;
        public QueryContext(HAPolicy haPolicy, HttpClient primaryClient, HttpClient secondaryClient) {
            this.haPolicy = haPolicy;
            this.primaryClient = primaryClient;
            this.secondaryClient = secondaryClient;
        }

        public boolean doQuery(){
            if (haPolicy.getRetryRule() == RetryRule.Primary || haPolicy.getRetryRule() == RetryRule.Secondary) {
                if (retryTimes < haPolicy.getQueryRetryTimes()) {
                    return true;
                }
            } else if (haPolicy.getRetryRule() == RetryRule.PrimaryPreferred || haPolicy.getRetryRule() == RetryRule.SecondaryPreferred) {
                if (retryTimes < haPolicy.getQueryRetryTimes() + 1) {
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
            switch (haPolicy.getRetryRule()) {
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

    public static class HAContext {
        private HAPolicy haPolicy;
        private HttpClient primaryClient;
        private HttpClient secondaryClient;
        private int retryTimes = 0;

        public HAContext(HAPolicy haPolicy, HttpClient primaryClient, HttpClient secondaryClient) {
            this.haPolicy = haPolicy;
            this.primaryClient = primaryClient;
            this.secondaryClient = secondaryClient;
        }

        protected void addRetryTimes() {
            retryTimes++;
            if (retryTimes > haPolicy.getCheckCount()) {
                HttpClient temp = secondaryClient;
                secondaryClient = primaryClient;
                primaryClient = temp;
                retryTimes = 0;
            }
        }

        protected void resetRetryTimes() {
            retryTimes = 0;
        }

        public HttpClient getPrimaryClient() {
            return primaryClient;
        }

        public HttpClient getSecondaryClient() {
            return secondaryClient;
        }
    }
}
