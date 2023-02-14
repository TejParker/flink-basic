package com.jarvis.wordcount;

import com.jarvis.wordcount.functions.MyFlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/*
    flink流批一体之：
        使用flink进行流处理
 */
public class WordCountStreamJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> wordsSource = env
                .readTextFile("data/words.txt")
                .name("source");

        DataStream<Tuple2<String, Integer>> wordCountStream = wordsSource
                .flatMap(new MyFlatMapFunction())
                .keyBy(in -> in.f0)
                .sum(1)
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .name("transform to word count");

        wordCountStream.print().name("sink to console");

        env.execute("Flink Word Count Batch");

    }
}
