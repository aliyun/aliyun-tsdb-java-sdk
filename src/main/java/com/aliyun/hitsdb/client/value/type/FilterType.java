package com.aliyun.hitsdb.client.value.type;

import java.lang.reflect.Field;

import com.aliyun.hitsdb.client.exception.http.HttpClientException;

public enum FilterType {
	LiteralOr("literal_or"),
    NotLiteralOr("not_literal_or"),
    Wildcard("wildcard"),
    Regexp("regexp"),
    GeoIntersects("intersects");

	private String name;

	private FilterType(String name) {
		this.name = name;
		Class<?> superclass = this.getClass().getSuperclass();
		try {
			Field field = superclass.getDeclaredField("name");
			field.setAccessible(true);
			field.set(this, name);
		} catch (Exception e) {
			// 理论上不会触发该异常。
			throw new HttpClientException(e);
		}
	}

	@Override
	public String toString() {
		return name;
	}

}

