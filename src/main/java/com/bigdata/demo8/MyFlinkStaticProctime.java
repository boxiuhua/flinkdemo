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
 * {"username":"zs","price":20}
 * {"username":"lisi","price":15}
 * {"username":"lisi","price":20}
 * {"username":"zs","price":20}
 * {"username":"zs","price":20}
 * {"username":"zs","price":20}
 * {"username":"zs","price":20}
 **/
public class MyFlinkStaticProctime {

    public static void main(String[] args) throws Exception {

        //1. env-准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.executeSql("CREATE TABLE table1 (\n" +
                "  `username` string,\n" +
                "  `price` int,\n" +
                "  `event_time` as PROCTIME()\n" +   //使用系统时间
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'topic1',\n" +
                "  'properties.bootstrap.servers' = '127.0.0.1:9092',\n" +
                "  'properties.group.id' = 'g1',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");

        // 编写sql语句

        tEnv.executeSql("select \n" +
                "   window_start,\n" +
                "   window_end,\n" +
                "   username,\n" +
                "   count(1) zongNum,\n" +
                "   sum(price) totalMoney \n" +
                "   from table(TUMBLE(TABLE table1, DESCRIPTOR(event_time), INTERVAL '60' second))\n" +
                " group by window_start,window_end,username").print();
    }
}