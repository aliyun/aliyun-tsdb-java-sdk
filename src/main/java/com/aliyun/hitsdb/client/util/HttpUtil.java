

package com.aliyun.hitsdb.client.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;

public class HttpUtil {
	/**
	 * Send a get request
	 * @param url
	 * @return response
	 * @throws IOException 
	 */
	static public String get(String url) throws IOException {
		return fetch("GET", url, null, null);
	}

	/**
	 * Send a request
	 * @param method      HTTP method, for example "GET" or "POST"
	 * @param url         Url as string
	 * @param body        Request body as string
	 * @param headers     Optional map with headers
	 * @return response   Response as string
	 * @throws IOException 
	 */
	static public String fetch(String method, String url, String body,
			Map<String, String> headers) throws IOException {
		// connection
		URL u = new URL(url);
		HttpURLConnection conn = (HttpURLConnection)u.openConnection();
		conn.setConnectTimeout(10000);
		conn.setReadTimeout(10000);

		// method
		if (method != null) {
			conn.setRequestMethod(method);
		}

		// headers
		if (headers != null) {
			for(String key : headers.keySet()) {
				conn.addRequestProperty(key, headers.get(key));
			}
		}

		// body
		if (body != null) {
			conn.setDoOutput(true);
			OutputStream os = conn.getOutputStream();
			os.write(body.getBytes());
			os.flush();
			os.close();
		}
		
		// response
		InputStream is = conn.getInputStream();
		String response = streamToString(is);
		is.close();
		
		// handle redirects
		if (conn.getResponseCode() == 301) {
			String location = conn.getHeaderField("Location");
			return fetch(method, location, body, headers);
		}
		
		return response;
	}
	
	/**
	 * Read an input stream into a string
	 * @param in
	 * @return
	 * @throws IOException
	 */
	static public String streamToString(InputStream in) throws IOException {
		StringBuffer out = new StringBuffer();
		byte[] b = new byte[4096];
		for (int n; (n = in.read(b)) != -1;) {
			out.append(new String(b, 0, n));
		}
		return out.toString();
	}
}
