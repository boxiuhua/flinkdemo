package com.bigdata.demo2;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.common.protocol.types.Field;

import java.util.Arrays;
import java.util.List;

/**
 * @基本功能:几个算子的综合案例
 * @program:flinkdemo
 * @author: 华哥
 * @create:2025-11-11 20:27:51
 **/
public class LearnStaticReduse {

    public static void main(String[] args) throws Exception {
        List<String> asList = Arrays.asList("tmd", "TMD", "wc", "他妈的");
        //1. env-准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2. source-加载数据
        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 8889);
        //3. transformation-数据处理转换
        dataStreamSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String line, Collector<String> out) throws Exception {
                String[] arr = line.split("\\s+");
                for (String word : arr) {
                    out.collect(word);
                }
            }
        }).filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String word) throws Exception {
                if(asList.contains(word)) {
                    return false;
                }
                return true;
            }
        }).map(new MapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return Tuple2.of(value,1);
            }
        }).keyBy(tuple -> tuple.f0)
                .reduce((tuple1,tuple2) -> Tuple2.of(tuple1.f0,tuple1.f1+tuple2.f1)).print();

        //4. sink-数据输出
        //5. execute-执行
        env.execute();
    }
}