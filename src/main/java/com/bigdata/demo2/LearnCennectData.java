package com.bigdata.demo2;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @基本功能:数据合并输出
 * @program:flinkdemo
 * @author: 华哥
 * @create:2025-11-11 21:03:09
 **/
public class LearnCennectData {

    public static void main(String[] args) throws Exception {

        //1. env-准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2. source-加载数据
        DataStreamSource<String> ds1 = env.fromElements("大数据", "javaWeb", "鸿蒙开发");
        DataStreamSource<String> ds2 = env.fromElements("Bigdata", "Springboot", "JS");
        DataStream<String> dataStream = ds1.union(ds2);
        //3. transformation-数据处理转换
        DataStreamSource<Long> ds3 = env.fromSequence(1, 10);
        //第一种实现
        dataStream.print();
        //第二种实现
        ConnectedStreams<String, Long> connectedStreams = ds1.connect(ds3);
        connectedStreams.map(new CoMapFunction<String, Long, String>() {
            @Override
            public String map1(String value) throws Exception {
                return value;
            }
            @Override
            public String map2(Long value) throws Exception {
                return String.valueOf(value);
            }
        }).print();
        //第三种实现
        ds1.connect(ds3).process(new CoProcessFunction<String, Long, String>() {
            @Override
            public void processElement1(String value, CoProcessFunction<String, Long, String>.Context ctx, Collector<String> out) throws Exception {
                out.collect(value);
            }
            @Override
            public void processElement2(Long value, CoProcessFunction<String, Long, String>.Context ctx, Collector<String> out) throws Exception {
                out.collect(String.valueOf(value));
            }
        }).print();
        //4. sink-数据输出
        //5. execute-执行
        env.execute();
    }
}