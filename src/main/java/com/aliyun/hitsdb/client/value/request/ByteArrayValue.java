package com.aliyun.hitsdb.client.value.request;

import com.alibaba.fastjson.JSONObject;

import java.util.Base64;

/**
 * @author cuiyuan
 * @date 2020/8/4 11:01 上午
 */

public class ByteArrayValue extends ComplexValue {
    static public final String TypeValue = "bytes";

    public ByteArrayValue(final byte[] content) {
        super(TypeValue, Base64.getEncoder().encodeToString(content));
    }

    public byte[] decode(){
        return Base64.getDecoder().decode(content);
    }

    public static boolean isJsonObjectTypeMatch(JSONObject jsonObject) {
        return jsonObject.containsKey(TypeKey) && jsonObject.containsKey(ContentKey) && jsonObject.size() == 2;
    }
}
