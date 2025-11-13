
package com.bigdata.demo4;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @基本功能: 数据往mysql插入数据
 * @program: flinkdemo
 * @author: 华哥
 * @create: 2025-11-13 13:40:44
 **/
public class LearnJdbcMyJdbcSink {

    public static void main(String[] args) throws Exception {
        //1. env-准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        DataStreamSource<Student> dataStreamSource = env.fromElements(
                new Student("张三4", 18),
                new Student("李四4", 19),
                new Student("王五4", 20)
        );

        // 将数据写入到mysql中
        dataStreamSource.addSink(new MyJdbcSink());

        env.execute();
    }
}