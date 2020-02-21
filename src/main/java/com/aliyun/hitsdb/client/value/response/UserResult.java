package com.aliyun.hitsdb.client.value.response;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.aliyun.hitsdb.client.value.JSONValue;

public class UserResult extends JSONValue {
    private String username;

    private String passcode;

    private int    privilege;

    public String getUserName() {
        return username;
    }

    public void setUserName(String userName) {
        this.username = userName;
    }

    public String getPasscode() {
        return passcode;
    }

    public void setPasscode(String base64Password) {
        this.passcode = base64Password;
    }

    public int getPrivilege() {
        return privilege;
    }

    public void setPrivilege(int privilege) {
        this.privilege = privilege;
    }

    public UserResult(String userName, String passcode, int privilege) {
        this.username = userName;
        this.passcode = passcode;
        this.privilege = privilege;
    }

    @Override
    public String toJSON() {
        return JSON.toJSONString(this, SerializerFeature.WriteNullStringAsEmpty);
    }
}
