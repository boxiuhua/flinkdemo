package com.bigdata.demo6;

import com.google.common.cache.*;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.concurrent.TimeUnit;

/**
 *
 * 假如维表中的数据比较多，不适合全部加载到map 中，可以使用如下方案
 *
 */
public class MytestJoinCache {

    public static void main(String[] args) throws Exception {
        //1. env-准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1);

        //2. source-加载数据
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("127.0.0.1:9092")
                .setTopics("first")
                .setGroupId("smart_jiaotong")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> dataStreamSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        //3. transformation-数据处理转换
        dataStreamSource.map(new RichMapFunction<String, String>() {


            Connection conn = null;
            PreparedStatement ps = null;

            // 定义一个Cache
            LoadingCache<String, String> cache;

            @Override
            public void open(Configuration parameters) throws Exception {
                // 这个里面可以编写连接数据库的代码
                // 这个里面编写连接数据库的代码
                Class.forName("com.mysql.jdbc.Driver");
                conn = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/flinktest", "root", "123456");
                ps = conn.prepareStatement("select * from city where city_id = ? ");

                cache = CacheBuilder.newBuilder()
                        //最多缓存个数，超过了就根据最近最少使用算法来移除缓存 LRU
                        .maximumSize(1000)
                        //在更新后的指定时间后就回收
                        // 不会自动调用，而是当过期后，又用到了过期的key值数据才会触发的。
                        .expireAfterWrite(100, TimeUnit.SECONDS)
                        //指定移除通知
                        .removalListener(new RemovalListener<String, String>() {
                            @Override
                            public void onRemoval(RemovalNotification<String, String> removalNotification) {
                                System.out.println(removalNotification.getKey() + "被移除了，值为：" + removalNotification.getValue());
                            }
                        })
                        .build(//指定加载缓存的逻辑
                                new CacheLoader<String, String>() {
                                    // 假如缓存中没有数据，会触发该方法的执行，并将结果自动保存到缓存中
                                    @Override
                                    public String load(String cityId) throws Exception {
                                        System.out.println("进入数据库查询啦。。。。。。。");
                                        ps.setString(1,cityId);
                                        ResultSet resultSet = ps.executeQuery();
                                        String cityName = null;
                                        if(resultSet.next()){
                                            System.out.println("进入到了if中.....");
                                            cityName = resultSet.getString("city_name");
                                        }
                                        return cityName;
                                    }
                                }
                        );

            }

            @Override
            public void close() throws Exception {
                ps.close();
                conn.close();

            }

            @Override
            public String map(String str) throws Exception {
                // zhangsan,1001
                String[] arr = str.split(",");
                String name = arr[0];
                String cityCode = arr[1];

                String cityName = "";
                if(cache.get(cityCode) != null){
                    cityName = cache.get(cityCode);
                }

                return name+","+cityCode+","+cityName;
            }
        }).print();


        //5. execute-执行
        env.execute();
    }
}
