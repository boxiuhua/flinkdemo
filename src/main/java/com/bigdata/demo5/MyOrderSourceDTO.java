package com.bigdata.demo5;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;
import java.util.UUID;

public class MyOrderSourceDTO implements SourceFunction<OrderInfo> {
    private boolean flag = true;
    @Override
    public void run(SourceContext<OrderInfo> ctx) throws Exception {
        Random random = new Random();
        while (flag){
            OrderInfo orderInfo = new OrderInfo();
            orderInfo.setOrderId(UUID.randomUUID().toString().replace("-",""));
            orderInfo.setUid(random.nextInt(3));
            orderInfo.setMoney(random.nextInt(101));
            orderInfo.setTimeStamp(System.currentTimeMillis()-10*random.nextInt(5)*1000);
            ctx.collect(orderInfo);
            Thread.sleep(1000);// 间隔1s
        }
    }

    @Override
    public void cancel() {
        flag = false;
    }
}
