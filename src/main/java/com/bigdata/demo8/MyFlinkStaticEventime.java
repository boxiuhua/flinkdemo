package com.bigdata.demo8;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @基本功能: 需求：按照滚动窗口和EventTime进行统计，每隔1分钟统计每个人的消费总额是多少
 * @program:flinkdemo
 * @author: 华哥
 * @create:2025-11-26 20:53:47
 *
 * 测试数据：按顺序输入
 * 注意：在本地运行时，默认的并行度是和你的cpu核数挂钩的，所以，为了快速看到结果，需要将并行度设置为1
 * 测试数据时，因为我们这个是eventTime,所以测试数据时，假如第一条数据是：
 * {"username":"zs","price":20,"event_time":"2023-07-17 10:10:10"}
 * 说明第一个窗口是 2023-07-17 10:10:00 ~ 2023-07-17 10:11:00
 * 因为有水印，水印时间是3秒，所以，要想触发第一个窗口有结果，必须出现条件是：
 * {"username":"zs","price":20,"event_time":"2023-07-17 10:11:03"}
 *
 *
 * {"username":"zs","price":20,"event_time":"2023-07-17 10:10:10"}
 * {"username":"zs","price":15,"event_time":"2023-07-17 10:10:30"}
 * {"username":"zs","price":20,"event_time":"2023-07-17 10:10:40"}
 * {"username":"zs","price":20,"event_time":"2023-07-17 10:11:03"}
 **/
public class MyFlinkStaticEventime {

    public static void main(String[] args) throws Exception {

        //1. env-准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.executeSql("CREATE TABLE table1 (\n" +
                "  `username` string,\n" +
                "  `price` int,\n" +
                "  `event_time` TIMESTAMP(3),\n" +
                "  watermark for event_time as event_time - interval '3' second\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'topic1',\n" +
                "  'properties.bootstrap.servers' = '127.0.0.1:9092',\n" +
                "  'properties.group.id' = 'g1',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");

        // 编写sql语句

        TableResult tableResult = tEnv.executeSql("select \n" +
                "   window_start,\n" +
                "   window_end,\n" +
                "   username,\n" +
                "   count(1) zongNum,\n" +
                "   sum(price) totalMoney \n" +
                "   from table(TUMBLE(TABLE table1, DESCRIPTOR(event_time), INTERVAL '60' second))\n" +
                "group by window_start,window_end,username");


        tableResult.print();

        env.execute();
    }
}