package com.jarvis.wordcount;

import com.jarvis.wordcount.functions.MyFlatMapFunction;
import org.apache.flink.api.common.eventtime.IngestionTimeAssigner;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
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
                .flatMap((String s , Collector<Tuple2<String, Integer>> out) -> {
                    for (String s1 : s.split(",")) {
                        out.collect(Tuple2.of(s1, 1));
                    }
                })
                // java lambda表达式使用了泛型，由于泛型类型擦除的原因，需要显示声明类型信息
                //.returns(Types.TUPLE(Types.STRING, Types.INT))
                .returns(TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}))
                .groupBy(0)
                .sum(1)
                .name("transform to word pair");
        /*DataSet<Tuple2<String, Integer>> wordCountStream = wordsSource
                .flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (s, collector) -> {
                    for (String s1 : s.split(",")) {
                        collector.collect(Tuple2.of(s1, 1));
                    }
                })
                .groupBy(0)
                .sum(1)
                .name("transform to word pair");*/

        wordCountStream.print();

        //env.execute("Flink Word Count Batch");

    }
}
