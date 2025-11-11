package com.bigdata.demo2;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.OutputStream;

/**
 * @基本功能:侧道输出流-可以分流 <br/>
 * 对流中的数据按照奇偶性进行分流，并获取分流后的数据
 * @program:flinkdemo
 * @author: 华哥
 * @create:2025-11-11 21:37:03
 **/
public class LearnSideStream {

    public static void main(String[] args) throws Exception {

        //1. env-准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        OutputTag<Long> tag_even = new OutputTag<>("偶数", TypeInformation.of(Long.class));
        OutputTag<Long> tag_odd = new OutputTag<>("奇数", TypeInformation.of(Long.class));

        //2. source-加载数据
        DataStreamSource<Long> dataStreamSource = env.fromSequence(1, 100);
        //3. transformation-数据处理转换
        SingleOutputStreamOperator<Long> process = dataStreamSource.process(new ProcessFunction<Long, Long>() {
            @Override
            public void processElement(Long value, ProcessFunction<Long, Long>.Context ctx, Collector<Long> out) throws Exception {
                if (value % 2 == 0) {
                    ctx.output(tag_even, value);
                } else {
                    ctx.output(tag_odd, value);
                }
            }
        });
        SideOutputDataStream<Long> enenStream = process.getSideOutput(tag_even);
        SideOutputDataStream<Long> oddStream = process.getSideOutput(tag_odd);


        //4. sink-数据输出
        enenStream.print("偶数");
        oddStream.print("奇数");
        //5. execute-执行
        env.execute();
    }
}