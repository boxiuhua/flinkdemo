
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
public class LearnJdbcSink {

    public static void main(String[] args) throws Exception {
        //1. env-准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        DataStreamSource<Student> dataStreamSource = env.fromElements(
                new Student("张三1", 18),
                new Student("李四1", 19),
                new Student("王五1", 20)
        );

        JdbcConnectionOptions jdbcConnectionOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withDriverName("com.mysql.cj.jdbc.Driver")
                .withUrl("jdbc:mysql://127.0.0.1:3306/codebind3?characterEncoding=UTF-8&useUnicode=true&useSSL=false&tinyInt1isBit=false&allowPublicKeyRetrieval=true&serverTimezone=Asia/Shanghai")
                .withUsername("root").withPassword("123456").build();
        // 将数据写入到mysql中
        dataStreamSource.addSink(JdbcSink.sink(
                "insert into stu values(null,?,?)", (JdbcStatementBuilder<Student>) (stat, student) -> {
                    stat.setString(1, student.getName());
                    stat.setInt(2, student.getAge());
                }, jdbcConnectionOptions
        ));

        env.execute();
    }
}