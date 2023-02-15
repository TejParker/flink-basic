package com.jarvis.wordcount;

import com.jarvis.wordcount.functions.MyFlatMapFunction;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/*
    flink流批一体之：
        使用flink进行流处理
 */
public class WordCountSocketJob {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 9081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        DataStreamSource<String> wordsSource = env
                .socketTextStream("localhost", 9999);

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
