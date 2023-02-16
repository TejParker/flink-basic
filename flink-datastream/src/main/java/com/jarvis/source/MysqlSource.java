package com.jarvis.source;

import com.jarvis.bean.Student;
import com.jarvis.utils.MysqlUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class MysqlSource extends RichSourceFunction<Student> {
    private Connection connection;
    private PreparedStatement pstmt;
    @Override
    public void open(Configuration parameters) throws Exception {
        connection = MysqlUtils.getConnection();
        pstmt = connection.prepareStatement("select id,name,age from student");
        System.out.println("Mysql Source open invoked");

    }

    @Override
    public void close() throws Exception {
        MysqlUtils.close(pstmt);
        MysqlUtils.close(connection);
        System.out.println("Mysql Source close invoked");
    }

    /**
     * 读取数据：把表中的数据，读取出来，转成Student
     * @param ctx
     * @throws Exception
     */
    @Override
    public void run(SourceContext<Student> ctx) throws Exception {
        ResultSet resultSet = pstmt.executeQuery();
        while (resultSet.next()) {
            int id = resultSet.getInt("id");
            String name = resultSet.getString("name");
            int age = resultSet.getInt("age");
            Student student = new Student(id, name, age);
            ctx.collect(student);
        }
    }

    @Override
    public void cancel() {
        System.out.println("Mysql Source cancel invoked");
    }
}
