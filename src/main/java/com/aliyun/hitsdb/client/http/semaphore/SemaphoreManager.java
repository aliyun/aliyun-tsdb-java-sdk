package com.aliyun.hitsdb.client.http.semaphore;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SemaphoreManager {
	private static final Logger LOGGER = LoggerFactory.getLogger(SemaphoreManager.class);
	private ConcurrentHashMap<String, Semaphore> addressSemaphoreMap;
	private int poolNum;
	private boolean putRequestLimitSwitch = true;

	private SemaphoreManager(List<String> addresses, int poolNum,boolean putRequestLimitSwitch) {
		synchronized (this) {
			this.poolNum = poolNum;
			this.addressSemaphoreMap = new ConcurrentHashMap<String, Semaphore>();
			for (String address : addresses) {
				Semaphore semaphore = new Semaphore(poolNum);
				this.addressSemaphoreMap.put(address, semaphore);
			}
			this.putRequestLimitSwitch = putRequestLimitSwitch;
		}
	}

	private SemaphoreManager(String address, int poolNum, boolean putRequestLimitSwitch) {
		this(Arrays.asList(address), poolNum,putRequestLimitSwitch);
	}

	public static SemaphoreManager create(List<String> addresses, int poolNum, boolean putRequestLimitSwitch) {
		SemaphoreManager semaphoreManager = new SemaphoreManager(addresses, poolNum, putRequestLimitSwitch);
		return semaphoreManager;
	}

	public static SemaphoreManager create(String address, int poolNum, boolean putRequestLimitSwitch) {
		SemaphoreManager semaphoreManager = new SemaphoreManager(address, poolNum, putRequestLimitSwitch);
		return semaphoreManager;
	}

	public void putAddress(String address) {
		this.addressSemaphoreMap.put(address, new Semaphore(poolNum));
	}

	public boolean acquire(String address) {
		if (!this.putRequestLimitSwitch) {
			return true;
		}

		Semaphore semaphore = this.addressSemaphoreMap.get(address);
		if (semaphore == null) {
			LOGGER.warn("the host:{} does not exist in the SemaphoreManager", address);
			return false;
		}

		for (int i = 0; i < 3; i++) {
			boolean acquire = semaphore.tryAcquire();
			if (acquire) {
				return acquire;
			}
		}

		return false;
	}

	public void release(String address) {
		if(!this.putRequestLimitSwitch){
			return;
		}
		Semaphore semaphore = this.addressSemaphoreMap.get(address);
		if (semaphore != null) {
			semaphore.release();
		}
	}

	public boolean removeAddress(String address) {
		Semaphore semaphore = this.addressSemaphoreMap.get(address);
		if (semaphore.availablePermits() == this.poolNum) {
			this.addressSemaphoreMap.remove(address);
			return true;
		}
		return false;
	}

	@Override
	public String toString() {
		return addressSemaphoreMap.toString();
	}

}
