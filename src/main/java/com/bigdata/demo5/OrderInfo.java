package com.bigdata.demo5;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class OrderInfo {

    private  String orderId;
    private  int uid;
    private  int money;
    private  long timeStamp;
}
