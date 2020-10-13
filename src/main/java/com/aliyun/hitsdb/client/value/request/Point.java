package com.aliyun.hitsdb.client.value.request;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.annotation.JSONField;
import com.alibaba.fastjson.annotation.JSONType;
import com.aliyun.hitsdb.client.HiTSDBConfig;
import com.aliyun.hitsdb.client.util.Objects;
import com.aliyun.hitsdb.client.value.JSONValue;
import com.aliyun.hitsdb.client.value.type.Granularity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@JSONType(ignores = {"granularityType"})
public class Point extends AbstractPoint {
    private static final Logger LOGGER = LoggerFactory.getLogger(Point.class);

    public static class MetricBuilder {
        private String metric;
        private Map<String, String> tags = new HashMap<String, String>();
        private Object value;
        private long timestamp;
        private Granularity granularityType;
        private String granularity;
        private String aggregator;
        private Long version;

        public MetricBuilder(final String metric) {
            this.metric = metric;
        }

        /**
         * add a TagKey and TagValue
         *
         * @param tagName tagName
         * @param value   value
         * @return MetricBuilder
         */
        public MetricBuilder tag(final String tagName, final String value) {
            Objects.requireNonNull(tagName, "tagName");
            Objects.requireNonNull(value, "value");
            if (!tagName.isEmpty()) {
                tags.put(tagName, value);
            } else {
                LOGGER.warn("Warning. Tag name is empty. We will ignore them as we cannot process the empty tag name.");
            }
            return this;
        }

        /**
         * add the tags
         *
         * @param tags a map
         * @return MetricBuilder
         */
        public MetricBuilder tag(final Map<String, String> tags) {
            if (tags != null) {
                this.tags.putAll(tags);
            }
            return this;
        }

        /**
         * set aggregator
         *
         * @param aggregator aggregator
         * @return MetricBuilder
         */
        public MetricBuilder aggregator(String aggregator) {
            this.aggregator = aggregator;
            return this;
        }

        /**
         * set timestamp
         *
         * @param timestamp time
         * @return MetricBuilder
         */
        public MetricBuilder timestamp(long timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        /**
         * set timestamp
         *
         * @param date java.util.Date
         * @return MetricBuilder
         */
        public MetricBuilder timestamp(Date date) {
            Objects.requireNonNull(date);
            this.timestamp = date.getTime();
            return this;
        }

        /**
         * set value
         *
         * @param value value
         * @return MetricBuilder
         */
        public MetricBuilder value(Object value) {
            Objects.requireNonNull(value);
            this.value = value;
            return this;
        }

        /**
         * set value
         *
         * @param timestamp timestamp
         * @param value     doube, long, int
         * @return MetricBuilder
         */
        public MetricBuilder value(long timestamp, Object value) {
            Objects.requireNonNull(value);
            this.timestamp = timestamp;
            this.value = value;
            return this;
        }

        /**
         * set date and value
         *
         * @param date  date
         * @param value doube, long, int
         * @return MetricBuilder
         */
        public MetricBuilder value(Date date, Object value) {
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


        /**
         * build a point
         *
         * @return Point
         */
        public Point build() {
            return build(true);
        }


        public Point build(boolean checkPoint) {
            Point point = new Point();
            point.metric = this.metric;
            point.tags = this.tags;
            point.timestamp = this.timestamp;
            point.value = this.value;
            point.granularity = this.granularity;
            point.aggregator = this.aggregator;
            point.version = this.version;

            if (checkPoint) {
                Point.checkPoint(point);
            }

            return point;
        }

        /**
         * convert to json
         *
         * @param point
         * @return String
         */
        private String buildJSON(Point point) {
            return JSON.toJSONString(point);
        }

    }


    /**
     * set the metric
     *
     * @param metric metric
     * @return MetricBuilder get a builder
     */
    public static MetricBuilder metric(String metric) {
        return new MetricBuilder(metric);
    }

    @JSONField(serializeUsing = ValueSerializer.class, deserializeUsing = ValueSerializer.class)
    private Object value;
    private String granularity;
    private String aggregator;
    private Long version;

    public Object getValue() {
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

    @Override
    public PointType getPointType() {
        return PointType.SINGLE_VALUE;
    }

    public void setValue(Object value) {
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


    /**
     * Checkout the point format
     *
     * @param point point
     */
    public static void checkPoint(Point point) {
        if (point.metric == null || point.metric.length() == 0) {
            throw new IllegalArgumentException("The metric can't be empty");
        }

        if (point.timestamp == null) {
            throw new IllegalArgumentException("The timestamp can't be null");
        }
        checkTimestamp(point.timestamp);
        if (point.value == null) {
            throw new IllegalArgumentException("The value can't be all null");
        }

        if (point.value instanceof Number && point.value == (Number) Double.NaN) {
            throw new IllegalArgumentException("The value can't be NaN");
        }

        if (point.value instanceof Number && point.value == (Number) Double.POSITIVE_INFINITY) {
            throw new IllegalArgumentException("The value can't be POSITIVE_INFINITY");
        }

        if (point.value instanceof Number && point.value == (Number) Double.NEGATIVE_INFINITY) {
            throw new IllegalArgumentException("The value can't be NEGATIVE_INFINITY");
        }

        for (int i = 0; i < point.metric.length(); i++) {
            final char c = point.metric.charAt(i);
            if (!checkChar(c)) {
                throw new IllegalArgumentException("There is an invalid character in metric. the char is '" + c + "'");
            }
        }
        if (point.tags == null || point.tags.isEmpty()) {
            return;
        }
        for (Entry<String, String> entry : point.tags.entrySet()) {
            String tagkey = entry.getKey();
            String tagvalue = entry.getValue();

            if (tagkey == null || tagkey.length() == 0) {
                throw new IllegalArgumentException("the tag key is null or empty");
            }

            if (tagvalue == null) {
                throw new IllegalArgumentException("the tag value is null");
            }

            for (int i = 0; i < tagkey.length(); i++) {
                final char c = tagkey.charAt(i);
                if (!checkChar(c)) {
                    throw new IllegalArgumentException("There is an invalid character in tagkey. the tagkey is + "
                            + tagkey + ", the char is '" + c + "'");
                }
            }

            for (int i = 0; i < tagvalue.length(); i++) {
                final char c = tagvalue.charAt(i);
                if (!checkChar(c)) {
                    throw new IllegalArgumentException("There is an invalid character in tagvalue. the tag is + <"
                            + tagkey + ":" + tagvalue + "> , the char is '" + c + "'");
                }
            }
        }
    }

}
