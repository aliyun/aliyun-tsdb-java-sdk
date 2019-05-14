package com.aliyun.hitsdb.client.http;

import com.aliyun.hitsdb.client.Config;

public class HttpAddressManager {
	private final String host;
	private final int port;
	private final String address;

	private HttpAddressManager(Config config) {
		host = config.getHost();
		port = config.getPort();
		address = host + ":" + port;
	}

	public static HttpAddressManager createHttpAddressManager(Config config) {
		return new HttpAddressManager(config);
	}

	public String getAddress() {
		return this.address;
	}
}
