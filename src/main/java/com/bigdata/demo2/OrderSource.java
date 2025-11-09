package com.bigdata.demo2;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;
import java.util.UUID;

/**
 * 需求: 每隔1秒随机生成一条订单信息(订单ID、用户ID、订单金额、时间戳)
 * 要求:
        * - 随机生成订单ID(UUID)
 * - 随机生成用户ID(0-2)
 * - 随机生成订单金额(0-100)
 * - 时间戳为当前系统时间
 */
//public class MyorderSource implements SourceFunction<OrderInfo> { //并行度1
//public class MyorderSource implements ParallelSourceFunction<OrderInfo> { //并行度=电脑核数
public class OrderSource extends RichParallelSourceFunction<OrderInfo> {
    boolean isRun = true;

    @Override
    public void run(SourceContext<OrderInfo> sourceContext) throws Exception {
        Random random = new Random();
        while (isRun){
            OrderInfo orderInfo = new OrderInfo();
            orderInfo.setOrderId(UUID.randomUUID().toString().replace("-",""));
            orderInfo.setUid(random.nextInt(10));
            orderInfo.setMoney(random.nextInt(10000));
            orderInfo.setTimeStamp(System.currentTimeMillis());
            sourceContext.collect(orderInfo);
            Thread.sleep(1000);//间隔1s

        }
    }

    @Override
    public void cancel() {
        isRun = false;
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println("此处可以进行数据库连接或者别的需要提前准备的操作。");
    }

    @Override
    public void close() throws Exception {
        System.out.println("此处可以做一些收尾的工作。");
    }
}
