package com.aliyun.hitsdb.client.http;

import java.util.ArrayList;
import java.util.List;

import com.aliyun.hitsdb.client.HiTSDBConfig;
import com.aliyun.hitsdb.client.balance.AtomicRoundInteger;
import com.aliyun.hitsdb.client.util.TSDBNetAddress;

public class HttpAddressManager {
	private final List<String> addresses;
	private AtomicRoundInteger ari;

	private HttpAddressManager(HiTSDBConfig config) {
	    List<TSDBNetAddress> netAddressList = config.getNetAddress();
	    this.addresses = new ArrayList<String>(netAddressList.size());
	    this.ari = new AtomicRoundInteger(this.addresses.size());
	    for(TSDBNetAddress netAddress : netAddressList) {
	        this.addresses.add(netAddress.getHost() + ":" + netAddress.getPort());
	    }
	}
	
	public static HttpAddressManager createHttpAddressManager(HiTSDBConfig config) {
		HttpAddressManager httpAddressManager = new HttpAddressManager(config);
		return httpAddressManager;
	}

	public String getAddress() {
	    int size = this.addresses.size();
	    if(size == 1) {
	        return this.addresses.get(0);
	    } else {
	        // 负载均衡
	        int index = this.ari.getAndNext();
	        return this.addresses.get(index);
	    }
	}
}
