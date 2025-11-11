package com.bigdata.demo2;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @基本功能:
 * @program:flinkdemo
 * @author: 华哥
 * @create:2025-11-09 22:14:17
 **/
public class MapandFlatmap {

    public static void main(String[] args) throws Exception {

        //1. env-准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2. source-加载数据
        //3. transformation-数据处理转换
        //4. sink-数据输出

        //5. execute-执行
        env.execute();
    }
}