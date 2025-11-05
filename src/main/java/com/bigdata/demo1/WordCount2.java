package com.bigdata.demo1;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 新增并行度
 */
public class WordCount2 {

    /**
     * 1、创建env环境
     * 2、source加载数据
     * 3、transformation-数据处理转换
     * 4、sink-数据输出
     * 5、execute-执行
     * @param args
     */
    public static void main(String[] args) throws Exception {

        //env环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //指定该任务按照那种执行，自动AUTOMATIC，批BATCH，流STREAMING
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        //并行度
        System.out.println(env.getParallelism());
        env.setParallelism(2);
        System.out.println(env.getParallelism());
        //加载数据
        DataStreamSource<String> dataStreamSource = env.fromElements("hello world bigdata", "hadoop spark hive bigdata","hello word spark");

        SingleOutputStreamOperator<String> fatetedMap = dataStreamSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String line, Collector<String> out) throws Exception {
                String[] arr = line.split(" ");
                for (String word : arr) {
                    out.collect(word);
                }
            }
        });
        //hello --> (hello,1)   2元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> map = fatetedMap.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String word) throws Exception {
                return Tuple2.of(word, 1);
            }
        }).setParallelism(3);
        System.out.println(map.getParallelism());

        KeyedStream<Tuple2<String, Integer>, String> keyedStream = map.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });
        //第二列"1"累加
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = keyedStream.sum(1).setParallelism(5);
        sum.print();
        System.out.println(sum.getParallelism());

        env.execute();


    }
}
