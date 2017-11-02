package com.alibaba.hitsdb.client.value.request;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.annotation.JSONType;
import com.alibaba.hitsdb.client.HiTSDBConfig;
import com.alibaba.hitsdb.client.value.JSONValue;
import com.alibaba.hitsdb.client.value.type.Granularity;

@JSONType(ignores = { "granularityType" })
public class Point extends JSONValue {

	public static class MetricBuilder {
		private String metric;
		private Map<String, String> tags = new HashMap<String, String>();
		private Number value;
		private long timestamp;
		private Granularity granularityType;
		private String granularity;
		private String aggregator;
		private Long version;

		public MetricBuilder() {
		}

		public MetricBuilder(final String metric) {
			this.metric = metric;
		}

		public MetricBuilder tag(final String tagName, final String value) {
			Objects.requireNonNull(tagName, "tagName");
			Objects.requireNonNull(value, "value");
			if (!tagName.isEmpty() && !value.isEmpty()) {
				tags.put(tagName, value);
			}
			return this;
		}

		public MetricBuilder tag(final Map<String, String> tags) {
			this.tags.putAll(tags);
			return this;
		}

		public MetricBuilder aggregator(String aggregator) {
			this.aggregator = aggregator;
			return this;
		}

		public MetricBuilder timestamp(long timestamp) {
			this.timestamp = timestamp;
			return this;
		}
		
		public MetricBuilder timestamp(Date date) {
			Objects.requireNonNull(date);
			this.timestamp = date.getTime();
			return this;
		}

		public MetricBuilder value(Number value) {
			Objects.requireNonNull(value);
			this.value = value;
			return this;
		}

		public MetricBuilder value(long timestamp, Number value) {
			Objects.requireNonNull(value);
			this.timestamp = timestamp;
			this.value = value;
			return this;
		}

		public MetricBuilder value(Date date, Number value) {
			Objects.requireNonNull(value);
			Objects.requireNonNull(date);
			this.timestamp = date.getTime();
			this.value = value;
			return this;
		}

		public MetricBuilder granularity(Granularity granularity) {
			if (granularity == null) {
				return this;
			}

			if (granularity.equals(Granularity.S1)) {
				return this;
			}

			this.granularityType = granularity;
			this.granularity = granularityType.getName();
			return this;
		}

		public MetricBuilder version(Long version) {
			this.version = version;
			return this;
		}

		public Point build() {
			Point point = new Point();
			point.metric = this.metric;
			point.tags = this.tags;
			point.timestamp = this.timestamp;
			point.value = this.value;
			point.granularity = this.granularity;
			point.aggregator = this.aggregator;
			point.version = this.version;
			if (HiTSDBConfig.Builder.ProducerThreadSerializeSwitch) {
				point.json = buildJSON(point);
			}

			return point;
		}

		private String buildJSON(Point point) {
			return JSON.toJSONString(point);
		}

	}

	public static MetricBuilder metric(String metric) {
		return new MetricBuilder(metric);
	}

	private String metric;
	private Map<String, String> tags;
	private Long timestamp;
	private Number value;
	private String granularity;
	private String aggregator;
	private String json;
	private Long version;

	public String getMetric() {
		return metric;
	}

	public Map<String, String> getTags() {
		return tags;
	}

	public Long getTimestamp() {
		return timestamp;
	}

	public Number getValue() {
		return value;
	}

	public String getAggregator() {
		return this.aggregator;
	}

	public Granularity getGranularityType() {
		if (this.granularity == null) {
			return Granularity.S1;
		}
		return Granularity.getEnum(this.granularity);
	}

	public String getGranularity() {
		return this.granularity;
	}

	public void setMetric(String metric) {
		this.metric = metric;
	}

	public void setTags(Map<String, String> tags) {
		this.tags = tags;
	}

	public void setTimestamp(Long timestamp) {
		this.timestamp = timestamp;
	}

	public void setValue(Number value) {
		this.value = value;
	}

	public void setGranularity(String granularity) {
		this.granularity = granularity;
	}

	public void setAggregator(String aggregator) {
		this.aggregator = aggregator;
	}

	public Long getVersion() {
		return version;
	}

	public void setVersion(Long version) {
		this.version = version;
	}

	@Override
	public String toJSON() {
		if (HiTSDBConfig.Builder.ProducerThreadSerializeSwitch) {
			return this.json;
		} else {
			return super.toJSON();
		}
	}

}
