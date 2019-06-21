package com.aliyun.hitsdb.client.value.response;

import com.aliyun.hitsdb.client.value.JSONValue;

import java.util.List;
import java.util.Map;

public class SQLResult extends JSONValue {
	List<String> columns;
	List<String> metadata;
	List<Map<String, String>> rows;

	public List<String> getColumns() {
		return columns;
	}

	public void setColumns(List<String> columns) {
		this.columns = columns;
	}

	public List<String> getMetadata() {
		return metadata;
	}

	public void setMetadata(List<String> metadata) {
		this.metadata = metadata;
	}

	public List<Map<String, String>> getRows() {
		return rows;
	}

	public void setRows(List<Map<String, String>> rows) {
		this.rows = rows;
	}
}
