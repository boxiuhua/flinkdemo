package com.bigdata.demo2;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;


public class OrderInfo {
    private  String orderId;
    private  int uid;
    private  int money;
    private  long timeStamp;

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public void setUid(int uid) {
        this.uid = uid;
    }

    public void setMoney(int money) {
        this.money = money;
    }

    public void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
    }

    public String getOrderId() {
        return orderId;
    }

    public int getUid() {
        return uid;
    }

    public int getMoney() {
        return money;
    }

    public long getTimeStamp() {
        return timeStamp;
    }
}
