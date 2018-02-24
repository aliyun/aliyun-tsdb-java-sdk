package com.aliyun.hitsdb.client.consumer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.apache.http.HttpResponse;
import org.apache.http.concurrent.FutureCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.aliyun.hitsdb.client.HiTSDBConfig;
import com.aliyun.hitsdb.client.callback.AbstractBatchPutCallback;
import com.aliyun.hitsdb.client.callback.BatchPutCallback;
import com.aliyun.hitsdb.client.callback.BatchPutDetailsCallback;
import com.aliyun.hitsdb.client.callback.BatchPutSummaryCallback;
import com.aliyun.hitsdb.client.callback.http.HttpResponseCallbackFactory;
import com.aliyun.hitsdb.client.http.HttpAPI;
import com.aliyun.hitsdb.client.http.HttpAddressManager;
import com.aliyun.hitsdb.client.http.HttpClient;
import com.aliyun.hitsdb.client.http.semaphore.SemaphoreManager;
import com.aliyun.hitsdb.client.queue.DataQueue;
import com.aliyun.hitsdb.client.value.request.Point;
import com.google.common.util.concurrent.RateLimiter;

public class BatchPutRunnable implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(BatchPutRunnable.class);

    /**
     * 缓冲队列
     */
    private final DataQueue dataQueue;

    /**
     * Http客户端
     */
    private final HttpClient hitsdbHttpClient;

    /**
     * 批量提交回调
     */
    private final AbstractBatchPutCallback<?> batchPutCallback;
    
    /**
     * 消费者队列控制器。
     * 在优雅关闭中，若消费者队列尚未结束，则CountDownLatch用于阻塞close()方法。
     */
    private final CountDownLatch countDownLatch; 
    
    /**
     * 每批次数据点个数
     */
    private int batchSize;

    /**
     * 批次提交间隔，单位：毫秒
     */
    private int batchPutTimeLimit;

    /**
     * 回调包装与构造工厂
     */
    private final HttpResponseCallbackFactory httpResponseCallbackFactory;

    private final HiTSDBConfig config;
    
    private final SemaphoreManager semaphoreManager;
    		
	private final HttpAddressManager httpAddressManager;

	private RateLimiter rateLimiter;
	
    public BatchPutRunnable(DataQueue dataQueue, HttpClient httpclient, HiTSDBConfig config,CountDownLatch countDownLatch, RateLimiter rateLimiter) {
        this.dataQueue = dataQueue;
        this.hitsdbHttpClient = httpclient;
        this.semaphoreManager = hitsdbHttpClient.getSemaphoreManager();
        this.httpAddressManager = hitsdbHttpClient.getHttpAddressManager();
        this.batchPutCallback = config.getBatchPutCallback();
        this.batchSize = config.getBatchPutSize();
        this.batchPutTimeLimit = config.getBatchPutTimeLimit();
        this.config = config;
        this.countDownLatch = countDownLatch;
        this.rateLimiter = rateLimiter;
        this.httpResponseCallbackFactory = hitsdbHttpClient.getHttpResponseCallbackFactory();
    }

    @Override
    public void run() {
        // 线程变量sb，paramsMap，waitPoint，readyClose 每个线程只有一组这样的变量。
        StringBuilder sb = null;
        if (HiTSDBConfig.Builder.ProducerThreadSerializeSwitch) {
            sb = new StringBuilder(2048 * batchSize);
        }

        Map<String, String> paramsMap = new HashMap<String, String>();
        if (this.batchPutCallback != null) {
            if (batchPutCallback instanceof BatchPutCallback) {
            } else if (batchPutCallback instanceof BatchPutSummaryCallback) {
                paramsMap.put("summary", "true");
            } else if (batchPutCallback instanceof BatchPutDetailsCallback) {
                paramsMap.put("details", "true");
            }
        }
        
        Point waitPoint = null;
        boolean readyClose = false;
        int waitTimeLimit = batchPutTimeLimit/3;
        long batchPutTimeLimitNano = batchPutTimeLimit*1000l;
        
        while (true) {
            if(readyClose && waitPoint == null) {
                break ;
            }
            
            long t0 = System.nanoTime();  // nano
            List<Point> pointList = new ArrayList<Point>(batchSize);
            if (waitPoint != null) {
                pointList.add(waitPoint);
                waitPoint = null;
            }
            
            for (int i = pointList.size(); i < batchSize; i++) {
                try {
                    Point point = dataQueue.receive(waitTimeLimit);
                    if (point != null) {
                    		if(this.rateLimiter != null) {
                    			this.rateLimiter.acquire();
                    		}
                    		pointList.add(point);
                    }
                    
                    long t1 = System.nanoTime();  // nano
                    if(t1-t0 > batchPutTimeLimitNano) {
                        break;
                    }
                		/*
	                	Point point = dataQueue.receive(this.batchPutTimeLimit);
	                if(point == null) {
	                		break;
	                }
	                pointList.add(point);
	                long t1 = System.nanoTime();  // nano
	                if(t1-t0 > batchPutTimeLimitNano) {
	                    break;
	                }
                		*/
                } catch (InterruptedException e) {
                    readyClose = true;
                    LOGGER.info("The thread {} is interrupted", Thread.currentThread().getName());
                    break;
                }
            }
            
            if (pointList.size() == 0 && !readyClose) {
                try {
                    Point newPoint = dataQueue.receive();
                    waitPoint = newPoint;
                    continue ;
                } catch (InterruptedException e) {
                    readyClose = true;
                    LOGGER.info("The thread {} is interrupted", Thread.currentThread().getName());
                }
            }
            
            if(pointList.size() == 0) {
                continue;
            }

            // 序列化
            String strJson = serialize(pointList, sb);
            
            // 发送
            sendHttpRequest(pointList,strJson,paramsMap);
        }
        
        if (readyClose) {
            this.countDownLatch.countDown();
            return ;
        }
    }
    
    private String getAddressAndSemaphoreAcquire() {
    		String address;
		while(true) {
			address = httpAddressManager.getAddress();
	        boolean acquire = this.semaphoreManager.acquire(address);
	    		if(!acquire) {
	    			continue;
	    		} else {
	    			break;
	    		}
		}
		
		return address;
    }
    
    
    private void sendHttpRequest(List<Point> pointList,String strJson,Map<String,String> paramsMap) {
        String address = getAddressAndSemaphoreAcquire();
    	
    	    // 发送
        if (this.batchPutCallback != null) {
            FutureCallback<HttpResponse> postHttpCallback = this.httpResponseCallbackFactory
                    .createBatchPutDataCallback (
                        address,
						this.batchPutCallback,
						pointList,
						config,
						config.getBatchPutRetryCount()
            );
            
            try {
                hitsdbHttpClient.postToAddress(address,HttpAPI.PUT, strJson, paramsMap, postHttpCallback);
            } catch (Exception ex) {
        			this.semaphoreManager.release(address);
        			this.batchPutCallback.failed(address, pointList, ex);
            }
        } else {
            FutureCallback<HttpResponse> noLogicBatchPutHttpFutureCallback = this.httpResponseCallbackFactory
                    .createNoLogicBatchPutHttpFutureCallback(
                    		address,
                    		pointList,
                    		config,
                    		config.getBatchPutRetryCount()
                    	);
            try {
                hitsdbHttpClient.postToAddress(address,HttpAPI.PUT, strJson, noLogicBatchPutHttpFutureCallback);
            } catch (Exception ex) {
            		this.semaphoreManager.release(address);
            		noLogicBatchPutHttpFutureCallback.failed(ex);
            }
        }
    }
    
    private String serialize(List<Point> pointList, StringBuilder sb) {
        // 复用StringBuilder
        if (HiTSDBConfig.Builder.ProducerThreadSerializeSwitch) {
            sb.setLength(0);
            sb.append('[');
            for (Point point : pointList) {
                sb.append(point.toJSON());
                sb.append(",");
            }
            sb.setCharAt(sb.length() - 1, ']');
            return sb.toString();
        } else {
            return JSON.toJSONString(pointList);
        }

    }

}