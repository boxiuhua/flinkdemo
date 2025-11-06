package com.bigdata.demo2;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.codehaus.janino.util.resource.FileResource;

/**
 * 读取本地文件
 */
public class WordCount5 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FileSource<String> fileSource = FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path("datas/wc.txt")).build();
        DataStreamSource<String> text = env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "fileSource");
        text.print();
        env.execute();
    }
}
