package com.bigdata.demo9;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @基本功能:实时套牌车分析
 * 当某个卡口中出现一辆行驶的汽车，我们可以通过摄像头识别车牌号，然后在5秒内，另外一个卡口也识别到了同样车牌的车辆，那么很有可能这两辆车之中有很大几率存在套牌车，
 * 因为一般情况下不可能有车辆在5秒内经过两个卡口。如果发现涉嫌套牌车，写入Mysql数据库(t_violation_list)的结果表中，在后面的模块中，
 * 可以对这些违法车辆进行实时轨迹跟踪。
 * 开发思路：根据车牌分组。然后使用状态记录车牌通过卡口的时间，如果两个时间小于5s，则记录为涉嫌套牌车。
 * 测试数据：
 * {"action_time":1682219447,"monitor_id":"0001","camera_id":"1","car":"豫A12345","speed":34.5,"road_id":"01","area_id":"20"}
 * {"action_time":1682219448,"monitor_id":"0002","camera_id":"1","car":"豫A12345","speed":84.5,"road_id":"01","area_id":"20"}
 * {"action_time":1682219548,"monitor_id":"0002","camera_id":"1","car":"豫A12345","speed":84.5,"road_id":"01","area_id":"20"}
 * @program:flinkdemo
 * @author: 华哥
 * @create:2025-11-28 22:07:34
 **/
public class ZhCarCards {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 获取tableEnv
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 获取kafka中的数据
        // {"action_time":1682219447,"monitor_id":"0001","camera_id":"1","car":"豫A12345","speed":34.5,"road_id":"01","area_id":"20"}
        tableEnv.executeSql("CREATE TABLE table1 (\n" +
                "  `action_time` BIGINT,\n" +
                "  `monitor_id` string,\n" +
                "  `camera_id` string,\n" +
                "  `car` string,\n" +
                "  `speed` double,\n" +
                "  `road_id` string,\n" +
                "  `area_id` string,\n" +
                "  `event_time`  as  TO_TIMESTAMP(FROM_UNIXTIME( action_time, 'yyyy-MM-dd HH:mm:ss')), \n"  +
                "   watermark for event_time as event_time - interval '0' second"  +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'topic-car',\n" +
                "  'properties.bootstrap.servers' = '127.0.0.1:9092',\n" +
                "  'properties.group.id' = 'g1232',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");
        // 因为需要将违章车辆插入违章表，所以创建一个mysql的表
        tableEnv.executeSql("CREATE TABLE t_violation_list (\n" +
                "  `car` string,\n" +
                "  `violation` string,\n" +
                "  `create_time` bigint \n" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:mysql://127.0.0.1:3306/smart_transportation?useUnicode=true&characterEncoding=utf8',\n" +
                "    'table-name' = 't_violation_list', \n" +
                "    'username' = 'root',\n" +
                "    'password' = '123456'\n" +
                ")");

        // 统计涉嫌套牌车辆  先根据车牌分组和窗口分组，并且是不同的monitor_id，然后统计次数大于等于2的
        tableEnv.executeSql(
                "insert into t_violation_list " +
                        "select \n" +
                        "   car,\n" +
                        "   cast('涉嫌套牌' as string) , \n" +
                        "   UNIX_TIMESTAMP()*1000 \n" +
                        "   from table(HOP(TABLE table1, DESCRIPTOR(event_time),INTERVAL '1' second, INTERVAL '5' second))\n" +
                        "group by window_start,window_end,car having count(distinct monitor_id) >= 2");
    }
}