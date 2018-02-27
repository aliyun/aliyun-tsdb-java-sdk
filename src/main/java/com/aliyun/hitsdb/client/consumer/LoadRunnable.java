package com.aliyun.hitsdb.client.consumer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import com.aliyun.hitsdb.client.HiTSDBConfig;
import com.aliyun.hitsdb.client.callback.LoadCallback;
import com.aliyun.hitsdb.client.callback.http.HttpResponseCallbackFactory;
import com.aliyun.hitsdb.client.http.HttpAPI;
import com.aliyun.hitsdb.client.http.HttpAddressManager;
import com.aliyun.hitsdb.client.http.HttpClient;
import com.aliyun.hitsdb.client.http.semaphore.SemaphoreManager;
import com.aliyun.hitsdb.client.tscompress.queue.CompressionBatchPointsQueue;
import com.aliyun.hitsdb.client.value.request.CompressionBatchPoints;
import com.aliyun.hitsdb.client.value.request.Point;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.http.HttpResponse;
import org.apache.http.concurrent.FutureCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoadRunnable implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoadRunnable.class);

    /**
     * 缓冲队列
     */
    private final CompressionBatchPointsQueue dataQueue;

    /**
     * Http客户端
     */
    private final HttpClient hitsdbHttpClient;

    /**
     * 批量提交回调
     */
    private final LoadCallback loadCallback;
    
    /**
     * 消费者队列控制器。
     * 在优雅关闭中，若消费者队列尚未结束，则CountDownLatch用于阻塞close()方法。
     */
    private final CountDownLatch countDownLatch; 
    
    /**
     * 回调包装与构造工厂
     */
    private final HttpResponseCallbackFactory httpResponseCallbackFactory;

    private final HiTSDBConfig config;
    
    private final SemaphoreManager semaphoreManager;
    		
	private final HttpAddressManager httpAddressManager;
	
    public LoadRunnable(CompressionBatchPointsQueue dataQueue, HttpClient httpclient, HiTSDBConfig config,CountDownLatch countDownLatch, RateLimiter rateLimiter) {
        this.dataQueue = dataQueue;
        this.hitsdbHttpClient = httpclient;
        this.semaphoreManager = hitsdbHttpClient.getSemaphoreManager();
        this.httpAddressManager = hitsdbHttpClient.getHttpAddressManager();
        this.loadCallback = config.getLoadCallback();
        this.config = config;
        this.countDownLatch = countDownLatch;
        this.httpResponseCallbackFactory = hitsdbHttpClient.getHttpResponseCallbackFactory();
    }

    @Override
    public void run() {
        Map<String, String> paramsMap = new HashMap<String, String>();
        
        Point waitPoint = null;
        boolean readyClose = false;
        
        while (true) {
            if(readyClose && waitPoint == null) {
                break ;
            }
            
            CompressionBatchPoints points;
            try {
                points = dataQueue.receive();
                // 发送
                sendHttpRequest(points,paramsMap);
            } catch (InterruptedException e) {
                readyClose = true;
                LOGGER.info("The thread {} is interrupted", Thread.currentThread().getName());
            }
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
    
    private void sendHttpRequest(CompressionBatchPoints points, Map<String,String> paramsMap) {
        String address = getAddressAndSemaphoreAcquire();
    	
    	    // 发送
        if (this.loadCallback != null) {
            FutureCallback<HttpResponse> postHttpCallback = this.httpResponseCallbackFactory
                    .createLoadCallback (
                        	address,
                        	this.loadCallback,
                        	points,
                        	config
                	);
            
            try {
                hitsdbHttpClient.postToAddress(address,HttpAPI.LOAD, points.getCompressData(), paramsMap, postHttpCallback);
            } catch (Exception ex) {
        			this.semaphoreManager.release(address);
        			this.loadCallback.failed(address, points, ex);
            }
        } else {
            FutureCallback<HttpResponse> noLogicLoadCallback = this.httpResponseCallbackFactory
                    .createNoLogicLoadCallback(
                    		address,
                    		points,
                    		config
                    	);
            try {
                hitsdbHttpClient.postToAddress(address,HttpAPI.LOAD, points.getCompressData(), noLogicLoadCallback);
            } catch (Exception ex) {
            		this.semaphoreManager.release(address);
            		noLogicLoadCallback.failed(ex);
            }
        }
    }

}