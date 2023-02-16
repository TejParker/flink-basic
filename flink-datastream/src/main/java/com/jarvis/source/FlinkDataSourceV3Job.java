package com.jarvis.source;

import com.jarvis.bean.Student;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkDataSourceV3Job {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Student> source = env.addSource(new MysqlSource());

        source.print();

        env.execute("Mysql 自定义Source");
    }
}
