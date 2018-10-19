package com.aliyun.hitsdb.client.value.response;

/**
 * Created By jianhong.hjh
 * Date: 2018/9/20
 */
public class KeyValue {

    private long timestamp;

    private Object value;

    public KeyValue() {
    }

    public KeyValue(long timestamp, Object value) {
        this.timestamp = timestamp;
        this.value = value;
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

    public double doubleValue(){
        if(value == null){
            throw new NullPointerException("the value is null");
        }
        if(value instanceof Number){
            return ((Number)value).doubleValue();
        }
        throw new RuntimeException("the value is " + value + " can't as double value");
    }

    public float floadValue(){
        if(value == null){
            throw new NullPointerException("the value is null");
        }
        if(value instanceof Number){
            return ((Number)value).floatValue();
        }
        throw new RuntimeException("the value is " + value + " can't as float value");
    }


    public long longValue(){
        if(value == null){
            throw new NullPointerException("the value is null");
        }
        if(value instanceof Number){
            return ((Number)value).longValue();
        }
        throw new RuntimeException("the value is " + value + " can't as long value");
    }

    public int intValue(){
        if(value == null){
            throw new NullPointerException("the value is null");
        }
        if(value instanceof Number){
            return ((Number)value).intValue();
        }
        throw new RuntimeException("the value is " + value + " can't as int value");
    }


    public short shortValue(){
        if(value == null){
            throw new NullPointerException("the value is null");
        }
        if(value instanceof Number){
            return ((Number)value).shortValue();
        }
        throw new RuntimeException("the value is " + value + " can't as short value");
    }

    public byte byteValue(){
        if(value == null){
            throw new NullPointerException("the value is null");
        }
        if(value instanceof Number){
            return ((Number)value).byteValue();
        }
        throw new RuntimeException("the value is " + value + " can't as byte value");
    }

    public boolean boolValue(){
        if(value == null){
            throw new NullPointerException("the value is null");
        }
        return Boolean.valueOf(value.toString());
    }

    public char charValue(){
        if(value == null){
            throw new NullPointerException("the value is null");
        }
        String string = value.toString();
        return string.charAt(0);
    }

    public String stringValue(){
        if(value == null){
            throw new NullPointerException("the value is null");
        }
        return value.toString();
    }

    @Override
    public String toString() {
        return "KeyValue{" +
                "timestamp=" + timestamp +
                ", value=" + value +
                '}';
    }
}
