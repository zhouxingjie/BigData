package com.youzan.spark.sql;

import java.io.Serializable;

/**
 * Created by xingjie.zhou on 2017/1/6.
 */
public class Record implements Serializable {

    private String key;
    private String val;

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getVal() {
        return val;
    }

    public void setVal(String val) {
        this.val = val;
    }
}
