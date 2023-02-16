package com.jarvis.wordcount;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/*
    flink流批一体之：
        动态传参给flink应用程序
 */
public class WordCountSocketArgsJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 通过命令行传递flink应用需要的参数
        ParameterTool tool = ParameterTool.fromArgs(args);
        String hostname = tool.get("hostname");
        int port = tool.getInt("port");
        DataStreamSource<String> wordsSource = env
                .socketTextStream(hostname, port);

        wordsSource
                .flatMap((String value, Collector<Tuple2<String, Integer>> out) -> {
                    String[] splits = value.split(",");
                    for (String split : splits) {
                        out.collect(Tuple2.of(split.trim(), 1));
                    }
                })
                .returns(new TypeHint<Tuple2<String, Integer>>() {}.getTypeInfo())
                .keyBy(value -> value.f0)
                .sum(1)
                .name("transform to word count").print();


        env.execute("Flink Word Count Socket Source");

    }
}
