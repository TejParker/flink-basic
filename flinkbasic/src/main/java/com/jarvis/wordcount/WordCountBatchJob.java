package com.jarvis.wordcount;

import com.jarvis.wordcount.functions.MyFlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;

/*
    flink流批一体之：
        使用flink进行流处理
 */
public class WordCountBatchJob {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> wordsSource = env
                .readTextFile("data/words.txt")
                .name("source");

        wordsSource
                .flatMap(new MyFlatMapFunction())
                .groupBy(0)
                .sum(1)
                .name("transform to word count").print();


        //env.execute("Flink Word Count Batch");

    }
}
