package com.jarvis.wordcount;

import com.jarvis.wordcount.functions.MyFlatMapFunction;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/*
    flink流批一体之：
        使用flink进行流处理
 */
public class WordCountStreamBatchJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        DataStreamSource<String> wordsSource = env
                .readTextFile("data/words.txt");

        wordsSource
                .flatMap(new MyFlatMapFunction())
                .keyBy(value -> value.f0)
                .sum(1)
                .name("transform to word count").print();


        env.execute("Flink Word Count Stream Batch Union");

    }
}
