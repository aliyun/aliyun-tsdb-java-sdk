package com.aliyun.hitsdb.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class DNSChecker {
    private final TSDBClient tsdbClient;
    private static final Logger LOGGER = LoggerFactory.getLogger(DNSChecker.class);
    private Set<String> ips;
    private final String host;
    private final int checkInterval;
    private final ScheduledExecutorService executorService;
    public DNSChecker(TSDBClient tsdbClient, String host, int checkInterval) {
        this.tsdbClient = tsdbClient;
        this.host = host;
        this.ips = getIps(host);
        this.checkInterval = checkInterval;
        this.executorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread( Runnable r) {
                Thread t = Executors.defaultThreadFactory().newThread(r);
                t.setDaemon(true);
                return t;
            }
        });
    }

    public void start() {
       executorService.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    checkDns();
                } catch (Throwable t) {
                    LOGGER.warn("error occurred in dnsChecker", t);
                }
            }
        }, checkInterval, checkInterval, TimeUnit.SECONDS);
    }


    private void checkDns() {
        Set<String> tmpIps = getIps(host);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(tmpIps.toString());
        }
        if (tmpIps.size() == 0) {
            // failed after 3 time retries, reset all connection
            tsdbClient.resetAllConnections();
            return;
        }

        if (ips.containsAll(tmpIps) && tmpIps.containsAll(ips)) {
            // dns not change, do nothing
            return ;
        }
        // dns changed ,reset all connections
        ips = tmpIps;
        tsdbClient.resetAllConnections();
    }

    private Set<String> getIps(String host) {
        for (int i = 0; i < 3; i++) {
            try {
                Set<String> ipAdds = new HashSet<String>();
                InetAddress[] inetAddresses = InetAddress.getAllByName(host);
                for (InetAddress inetAddress : inetAddresses) {
                    String ip = inetAddress.getHostAddress();
                    ipAdds.add(ip);
                }
                return ipAdds;
            } catch (UnknownHostException e) {
                try {
                    // fail will retry after some time
                    TimeUnit.MILLISECONDS.sleep((i + 1) * 500);
                } catch (InterruptedException ignore) {
                }
                LOGGER.warn("resolve dns {} failed {}",host, e);
            }
        }
        LOGGER.error("resolve dns {} failed after retry",host);
        return new HashSet<String>();
    }

}
