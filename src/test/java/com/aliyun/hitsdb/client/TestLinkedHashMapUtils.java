package com.aliyun.hitsdb.client;

import java.util.LinkedHashMap;
import java.util.Map.Entry;

import org.junit.Before;
import org.junit.Test;

import com.aliyun.hitsdb.client.util.LinkedHashMapUtils;

public class TestLinkedHashMapUtils {
    
    LinkedHashMap<String, Integer> linkedHashMap;
    
    @Before
    public void init() {
        linkedHashMap = new LinkedHashMap<String, Integer>();
        linkedHashMap.put("one", 1);
//        linkedHashMap.put("two", 2);
//        linkedHashMap.put("three", 3);
//        linkedHashMap.put("four", 4);
    }
    
    @Test
    public void testTraverse(){
        for(Entry<String, Integer> entry:linkedHashMap.entrySet()){
            System.out.println(
                    entry.getKey() + ":" + entry.getValue()
            );
        }
    }
    
    @Test
    public void testGetTail() {
        Entry<String, Integer> entry = LinkedHashMapUtils.getTail(linkedHashMap);
        System.out.println(
                entry.getKey() + ":" + entry.getValue()
        );
    }
    
    @Test
    public void testGetTailBefore() {
        Entry<String, Integer> tailEntry = LinkedHashMapUtils.getTail(linkedHashMap);
        Entry<String, Integer> entry = LinkedHashMapUtils.getBefore(tailEntry);
        
        if(entry != null){
            System.out.println(
                    entry.getKey() + ":" + entry.getValue()
            );
        } else {
            System.out.println(entry);
        }
        
    }
}
