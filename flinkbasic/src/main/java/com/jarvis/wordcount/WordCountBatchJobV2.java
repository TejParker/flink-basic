package com.jarvis.wordcount;

import com.jarvis.wordcount.functions.MyFlatMapFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/*
    flink流批一体之：
        使用flink进行流处理
 */
public class WordCountBatchJobV2 {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> wordsSource = env
                .readTextFile("data/words.txt")
                .name("source");

        DataSet<Tuple2<String, Integer>> wordCountStream = wordsSource
                .flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (s, collector) -> {
                    for (String s1 : s.split(",")) {
                        collector.collect(Tuple2.of(s1, 1));
                    }
                })
                .groupBy(0)
                .sum(1)
                .name("transform to word count");

        wordCountStream.print();

        env.execute("Flink Word Count Batch");

    }
}
