package com.bigdata.demo2;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * kafka学习测试
 */
public class LearnOrderSource {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<OrderInfo> orderInfoDataStreamSource = env.addSource(new OrderSource()).setParallelism(1);
        System.out.println(orderInfoDataStreamSource.getParallelism());
        orderInfoDataStreamSource.print();
        env.execute();
    }
}
