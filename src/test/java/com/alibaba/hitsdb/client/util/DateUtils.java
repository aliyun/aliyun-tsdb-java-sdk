package com.alibaba.hitsdb.client.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateUtils {
    public static Date toDate(String strDate){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = null;
        try {
            date = sdf.parse(strDate);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return date;
    }
    
    public static Date now() {
        return new Date();
    }
    
    public static Date add(Date date,long timestamp){
        Calendar instance = Calendar.getInstance();
        instance.setTimeInMillis(date.getTime() + timestamp);
        Date time = instance.getTime();
        return time;
    }
    
    public static int toTimestampSecond(Date date){
        int second = (int)(date.getTime()/1000);
        return second;
    }
    
}
