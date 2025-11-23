package com.bigdata.demo5;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @基本功能: 使用Watermark + AllowedLateness + SideOutput ,即使用侧道输出机制来单独收集延迟/迟到/乱序严重的数据,避免数据丢失!
 * @program:flinkdemo
 * @author: 华哥
 * @create:2025-11-22 21:31:55
 **/
public class MyallowLateness {

    public static void main(String[] args) throws Exception {
        //1. env-准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
        DataStreamSource<OrderInfo> orderInfoDataStreamSource = env.addSource(new MyOrderSourceDTO());

        OutputTag<OrderInfo> sideOutput = new OutputTag<>("side output"){};

        SingleOutputStreamOperator<String> streamOperator = orderInfoDataStreamSource
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
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .allowedLateness(Time.seconds(5))
                .sideOutputLateData(sideOutput)
                .apply(new WindowFunction<OrderInfo, String, Integer, TimeWindow>() {
                    @Override
                    public void apply(Integer integer, TimeWindow window, Iterable<OrderInfo> input, Collector<String> out) throws Exception {
                        long start = window.getStart();
                        long end = window.getEnd();
                        String startStr = DateFormatUtils.format(start, "yyyy-MM-dd HH:mm:ss");
                        String endStr = DateFormatUtils.format(end, "yyyy-MM-dd HH:mm:ss");
                        int sumMoney = 0;
                        for (OrderInfo orderInfo : input) {
                            sumMoney += orderInfo.getMoney();
                        }
                        out.collect("开始时间："+startStr+"结束时间："+endStr+"，用户id="+integer+",订单总额："+sumMoney);
                    }
                });
        streamOperator.print("正常与迟到数据>>>>");
        streamOperator.getSideOutput(sideOutput).print("严重迟到数据>>>");



        env.execute();

    }
}