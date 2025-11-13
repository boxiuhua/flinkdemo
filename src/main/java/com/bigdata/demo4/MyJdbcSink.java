package com.bigdata.demo4;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;

/**
 * 自定义jdbc数据库保存
 */
public class MyJdbcSink extends RichSinkFunction<Student> {
    Connection conn = null;
    PreparedStatement ps = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName("com.mysql.jdbc.Driver");
        conn = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/codebind3", "root", "123456");
        ps = conn.prepareStatement("INSERT INTO `stu` ( `name`, `age`) VALUES ( ?, ?)");
    }

    @Override
    public void close() throws Exception {
        ps.close();
        conn.close();
    }

    @Override
    public void invoke(Student stu, Context context) throws Exception {
//        ps.setInt(1, stu.getId());
        ps.setString(1, stu.getName());
        ps.setInt(2, stu.getAge());
        ps.execute();
    }
}
