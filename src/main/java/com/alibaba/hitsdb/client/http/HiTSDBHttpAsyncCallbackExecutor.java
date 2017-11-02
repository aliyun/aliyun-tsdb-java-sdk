package com.alibaba.hitsdb.client.http;

import java.io.IOException;

import org.apache.http.Header;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.nio.NHttpClientConnection;
import org.apache.http.nio.protocol.HttpAsyncRequestExecutor;
import org.apache.http.protocol.HttpContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiTSDBHttpAsyncCallbackExecutor extends HttpAsyncRequestExecutor {
	private static final Logger LOGGER = LoggerFactory.getLogger(HiTSDBHttpAsyncCallbackExecutor.class);
	private final int liveTime;

	public HiTSDBHttpAsyncCallbackExecutor(int liveTime) {
		super();
		this.liveTime = liveTime;
	}

	@Override
	public void requestReady(NHttpClientConnection conn) throws IOException, HttpException {
		try {
			super.requestReady(conn);
		} catch (Exception ex) {
			LOGGER.error("", ex);
		}

		// 需要自动关闭连接
		if (this.liveTime > 0) {
			HttpRequest httpRequest = conn.getHttpRequest();
			if (httpRequest == null) {
				return;
			}

			HttpContext context = conn.getContext();

			long currentTimeMillis = System.currentTimeMillis();
			Object oldTimeMillisObj = context.getAttribute("t");
			if (oldTimeMillisObj == null) {
				context.setAttribute("t", currentTimeMillis);
			} else {
				long oldTimeMillis = (Long) oldTimeMillisObj;
				long dt = currentTimeMillis - oldTimeMillis;
				if (dt > 1000 * liveTime) { // 超时，重连
					tryCloseConnection(httpRequest);
					context.setAttribute("t", currentTimeMillis);
				}
			}
		}
	}

	private void tryCloseConnection(HttpRequest request) {
		Header[] headers = request.getHeaders("Connection");
		if (headers != null && headers.length > 0) {
			for (Header h : headers) {
				request.removeHeader(h);
			}
		}

		request.addHeader("Connection", "close");
	}

}
