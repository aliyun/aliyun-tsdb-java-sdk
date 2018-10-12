package com.aliyun.hitsdb.client.util;

import java.util.Properties;

public class PropKit {

    private Properties properties;

    public PropKit(Properties properties){
        this.properties = properties;
    }

    public String get(String key){
        return properties.getProperty(key);
    }

    public boolean containsKey(String key){
        return properties.containsKey(key);
    }

    public Integer getInt(String key){
        return Integer.parseInt(properties.getProperty(key));
    }

    public Boolean getBoolean(String key){
        return Boolean.parseBoolean(properties.getProperty(key));
    }
}
