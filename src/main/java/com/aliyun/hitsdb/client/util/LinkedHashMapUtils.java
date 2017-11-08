package com.aliyun.hitsdb.client.util;

import java.lang.reflect.Field;
import java.util.LinkedHashMap;
import java.util.Map.Entry;

import com.aliyun.hitsdb.client.exception.http.HttpClientException;

public class LinkedHashMapUtils {
    
    private static final Field tailField;
    private static final Field beforeField;
    
    static {
        try {
            tailField = LinkedHashMap.class.getDeclaredField("tail");
            tailField.setAccessible(true);
        } catch (NoSuchFieldException e) {
            throw new LinkedHashMapException(e);
        } catch (SecurityException e) {
            throw new LinkedHashMapException(e);
        }
        
        try {
            Class<?> clazz = Class.forName("java.util.LinkedHashMap$Entry");
            beforeField = clazz.getDeclaredField("before");
            beforeField.setAccessible(true);
        } catch (ClassNotFoundException e) {
            throw new LinkedHashMapException(e);
        } catch (NoSuchFieldException e) {
            throw new LinkedHashMapException(e);
        } catch (SecurityException e) {
            throw new LinkedHashMapException(e);
        }
        
    }
    

    @SuppressWarnings("unchecked")
    public static <K, V> Entry<K, V> getTail(LinkedHashMap<K, V> map) throws LinkedHashMapException {
        try {
            Entry<K, V> entry = (Entry<K, V>) tailField.get(map);
            return entry;
        } catch (SecurityException e) {
            throw new LinkedHashMapException(e);
        } catch (IllegalArgumentException e) {
            throw new LinkedHashMapException(e);
        } catch (IllegalAccessException e) {
            throw new LinkedHashMapException(e);
        }
    }

    @SuppressWarnings("unchecked")
    public static <K, V> Entry<K, V> getBefore(Entry<K, V> entry) throws LinkedHashMapException {
        try {
            Entry<K, V> beforeEntry = (Entry<K, V>) beforeField.get(entry);
            return beforeEntry;
        } catch (SecurityException e) {
            throw new LinkedHashMapException(e);
        } catch (IllegalArgumentException e) {
            throw new LinkedHashMapException(e);
        } catch (IllegalAccessException e) {
            throw new LinkedHashMapException(e);
        }
    }

}

class LinkedHashMapException extends HttpClientException {
    private static final long serialVersionUID = 1L;
    
    LinkedHashMapException(Exception e){
        super(e);
    }
    
}
