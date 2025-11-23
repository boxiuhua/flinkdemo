package com.bigdata.demo7;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @基本功能:flinkSQL初认识
 * @program:flinkdemo
 * @author: 华哥
 * @create:2025-11-23 18:07:25
 **/
public class FlinkSqlTest1 {

    public static void main(String[] args) throws Exception {

        //1. env-准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 7788);

        // 将这个流对象变为一个表
        tableEnv.createTemporaryView("table1", dataStreamSource);
        //TableResult tableResult = tableEnv.executeSql("select * from table1");
        //tableResult.print();
        Table table = tableEnv.sqlQuery("select * from table1");
        /**
         * 列明：f0
         * (
         *   `f0` STRING
         * )
         */
        table.printSchema();// Table 对象，不能打印结果，只能打印表结构
        DataStream<Row> dataStream = tableEnv.toAppendStream(table, Row.class);
        // stream流如何打印？ print
        dataStream.print();
        //5. execute-执行
        env.execute();
    }
}