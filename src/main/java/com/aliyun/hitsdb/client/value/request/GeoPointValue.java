package com.aliyun.hitsdb.client.value.request;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.annotation.JSONField;
import com.aliyun.hitsdb.client.util.WKTParser;

import java.text.ParseException;
import java.util.Objects;


public class GeoPointValue extends ComplexValue {
    static public final String TypeValue = "geopoint";

    @JSONField(serialize = false)
    private double longitude;

    @JSONField(serialize = false)
    private double latitude;

    public GeoPointValue(final String content) {
        super(TypeValue, content);
        try {
            WKTParser strParser = new WKTParser(content);
            strParser.moveToExpect('(');
            longitude = strParser.nextDouble();
            latitude = strParser.nextDouble();
            strParser.nextExpect(')');
        } catch (ParseException e) {
            throw new IllegalArgumentException("Failed to parse the geopoint string literal: " + content);
        }
        if (longitude < -180 || longitude > 180 || latitude < -90 || latitude > 90) {
            throw new IllegalArgumentException("Longitude or latitude is out of valid range: " + content);
        }
    }

    private GeoPointValue(final String content, boolean unused) {
        super(TypeValue, content);
    }

    public static GeoPointValue fromLonLat(double longitude, double latitude) {
        if (longitude < -180 || longitude > 180 || latitude < -90 || latitude > 90) {
            throw new IllegalArgumentException("Longitude or latitude is out of valid range.");
        }
        GeoPointValue result = new GeoPointValue(String.format("POINT (%f %f)", longitude, latitude), false);
        result.longitude = longitude;
        result.latitude = latitude;
        return result;
    }

    public static GeoPointValue fromWKT(String wkt) {
        return new GeoPointValue(wkt);
    }

    public double getLongitude() {
        return longitude;
    }

    public double getLatitude() {
        return latitude;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || !(o instanceof GeoPointValue)) {
            return false;
        }
        return Double.compare(longitude, ((GeoPointValue) o).longitude) == 0
                && Double.compare(latitude, ((GeoPointValue) o).latitude) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(longitude, latitude);
    }

    @Override
    public String toString() {
        return content;
    }

    public static boolean isJsonObjectTypeMatch(JSONObject jsonObject) {
        return ComplexValue.isJsonObjectTypeMatch(jsonObject) && jsonObject.get(TypeKey).equals(TypeValue);
    }

}
