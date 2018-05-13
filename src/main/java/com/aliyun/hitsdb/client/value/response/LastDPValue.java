package com.aliyun.hitsdb.client.value.response;

import java.util.Map;

public class LastDPValue {
	private String metric;
	private long timestamp;
	private Object value;
	private Map<String, String> tags;
	private int version;

	public String getMetric() {
		return metric;
	}

	public void setMetric(String metric) {
		this.metric = metric;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public Object getValue() {
		return value;
	}

	public void setValue(Object value) {
		this.value = value;
	}

	public Map<String, String> getTags() {
		return tags;
	}

	public void setTags(Map<String, String> tags) {
		this.tags = tags;
	}

	public int getVersion() {
		return version;
	}

	public void setVersion(int version) {
		this.version = version;
	}

	@Override
	public String toString() {
		return "LastDPValue [metric=" + metric + ", timestamp=" + timestamp + ", value=" + value + ", tags=" + tags
				+ ", version=" + version + "]";
	}

}
