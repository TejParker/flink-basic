package com.jarvis.wordcount;

import com.jarvis.wordcount.functions.MyFlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

/*
    flink流批一体之：
        使用flink进行流处理
 */
public class WordCountBatchJob {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> wordsSource = env
                .readTextFile("data/words.txt")
                .name("source");

        DataSet<Tuple2<String, Integer>> wordCountStream = wordsSource
                .flatMap(new MyFlatMapFunction())
                .groupBy(0)
                .sum(1)
                .name("transform to word count");

        wordCountStream.print();

        env.execute("Flink Word Count Batch");

    }
}
