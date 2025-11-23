package com.bigdata.demo6;

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
import java.util.HashMap;
import java.util.Map;

/**
 *
 * 通过定义一个类实现RichMapFunction，在open()中读取维表数据加载到内存中，在kafka流map()方法中与维表数据进行关联。
 * RichMapFunction中open方法里加载维表数据到内存的方式特点如下：
 * ● 优点：实现简单
 * ● 缺点：因为数据存于内存，所以只适合小数据量并且维表数据更新频率不高的情况下。虽然可以在open中定义一个定时器定时更新维表，但是还是存在维表更新不及时的情况。另外，维表是变化慢，不是一直不变的，只是变化比较缓慢而已。
 */
public class MytestJoin {

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

            Map<String, String> map = new HashMap<String, String>();

            ResultSet resultSet = null;

            @Override
            public void open(Configuration parameters) throws Exception {

                // 这个里面可以编写连接数据库的代码
                // 这个里面编写连接数据库的代码
                Class.forName("com.mysql.jdbc.Driver");
                conn = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/flinktest", "root", "123456");
                ps = conn.prepareStatement("select * from city ");
                resultSet = ps.executeQuery();
                while (resultSet.next()) {
                    String cityCode = resultSet.getString("city_id");
                    String cityName = resultSet.getString("city_name");
                    System.out.println(cityName + ":" + cityCode);
                    map.put(cityCode, cityName);
                }

            }

            @Override
            public void close() throws Exception {
                resultSet.close();
                ps.close();
                conn.close();

            }

            @Override
            public String map(String str) throws Exception {
                // zhangsan,1001
                String[] arr = str.split(",");
                String name = arr[0];
                String cityCode = arr[1];
                String cityName = map.get(cityCode);


                return name + "," + cityCode + "," + cityName;
            }
        }).print();


        //5. execute-执行
        env.execute();
    }
}
