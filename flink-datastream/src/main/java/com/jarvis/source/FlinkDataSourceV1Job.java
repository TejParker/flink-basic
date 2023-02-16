package com.jarvis.source;

import com.jarvis.bean.Visit;
import com.jarvis.source.functions.MySourceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkDataSourceV1Job {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Visit> source = env.addSource(new MySourceFunction());
        System.out.println(source.getParallelism());
        source.print();

        env.execute("自定义数据源");
    }
}
