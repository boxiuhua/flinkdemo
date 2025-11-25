package com.bigdata.demo8;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @基本功能: 需求: 从Kafka的topic1中消费数据并过滤出状态为success的数据再写入到MySQL
 * @program:flinkdemo
 * @author: 华哥
 * @create:2025-11-25 22:08:50
 **/
public class FlinkKafkaToMysql {

    public static void main(String[] args) throws Exception {

        //1. env-准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        // 获取tableEnv对象
        // 通过env 获取一个table 环境
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.executeSql("CREATE TABLE table1 (\n" +
                "  `user_id` int,\n" +
                "  `page_id` int,\n" +
                "  `status` STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'topic1',\n" +
                "  'properties.bootstrap.servers' = '127.0.0.1:9092',\n" +
                "  'properties.group.id' = 'g1',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");

        tEnv.executeSql("CREATE TABLE table2 (\n" +
                "  `user_id` int,\n" +
                "  `page_id` int,\n" +
                "  `status` STRING\n" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:mysql://127.0.0.1:3306/flinktest?useUnicode=true&characterEncoding=utf8',\n" +
                "    'table-name' = 't_success', \n" +
                "    'username' = 'root',\n" +
                "    'password' = '123456'\n" +
                ")");

        tEnv.executeSql(" insert into table2 select * from table1 where status='success'");


    }
}