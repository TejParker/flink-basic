package com.jarvis.source;

import com.jarvis.bean.Visit;
import com.jarvis.source.functions.MySourceFunction;
import com.jarvis.source.functions.MySourceFunctionV2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkDataSourceV2Job {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Visit> source = env.addSource(new MySourceFunctionV2()).setParallelism(3);
        System.out.println(source.getParallelism());
        source.print();

        env.execute("自定义数据源-多并行度");
    }
}
