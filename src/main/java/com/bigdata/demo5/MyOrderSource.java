package com.bigdata.demo5;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 *
 * 需求
 * 实时模拟生成订单数据,格式为: (订单ID，用户ID，时间戳/事件时间，订单金额)
 * 要求每隔5s,计算5秒内，每个用户的订单总金额
 * 并添加Watermark来解决一定程度上的数据延迟和数据乱序问题。
 * 不使用水印的时候【不能使用eventtime时间语义】，进行开发
 *
 */

public class MyOrderSource {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<OrderInfo> orderInfoDataStreamSource = env.addSource(new MyOrderSourceDTO());

        //使用TumblingProcessingTimeWindows
//        orderInfoDataStreamSource.keyBy(OrderInfo::getUid)
//                .window(TumblingProcessingTimeWindows.of(Time.seconds(5))).sum("money").print();

        //使用TumblingEventTimeWindows
        orderInfoDataStreamSource
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<OrderInfo>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
                            // long 是时间戳吗？是秒值还是毫秒呢？年月日时分秒的的字段怎么办呢？
                            @Override
                            public long extractTimestamp(OrderInfo orderInfo, long l) {
                                // 这个方法的返回值是毫秒，所有的数据只要不是这个毫秒值，都需要转换为毫秒
                                return orderInfo.getTimeStamp();
                            }
                        }))
                .keyBy(OrderInfo::getUid)
                .window(TumblingEventTimeWindows.of(Time.seconds(5))).sum("money").print();


        env.execute();
    }

}
