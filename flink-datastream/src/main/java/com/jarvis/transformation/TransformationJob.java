package com.jarvis.transformation;

import com.jarvis.bean.Visit;
import com.jarvis.functions.MyPartitioner;
import com.jarvis.functions.VisitConvertFunction;
import com.jarvis.functions.VisitDomainKeySelector;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Locale;

public class TransformationJob {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        DataStreamSource<String> source = env.readTextFile("./data/access.log");

        SingleOutputStreamOperator<Visit> visitStream = source.map(new VisitConvertFunction());

        //visitStream.filter(new VisitFilterFunction()).print();

        //visitStream.flatMap(new VisitFlatMapFunction()).print();

        //visitStream.keyBy(new VisitDomainKeySelector()).sum("traffic").print();

        visitStream.print();

        //DataStreamSource<Integer> stream1 = env.fromElements(1, 2, 3);
        //DataStreamSource<Integer> stream2 = env.fromElements(11, 22, 33);
        //DataStreamSource<String> stream3 = env.fromElements("A", "B", "C");

        //stream1.union(stream3).print();
        //stream1.union(stream2).map(x -> "union-" + x).print();
        /**
         * union:数据类型可以不同
         */
        /*ConnectedStreams<Integer, String> connectedStreams = stream2.connect(stream3);
        connectedStreams.map(new CoMapFunction<Integer, String, String>() {
            String prefix = "Connect_";
            @Override
            public String map1(Integer value) throws Exception {
                return prefix + value * 10;
            }

            @Override
            public String map2(String s) throws Exception {
                return prefix + s.toUpperCase();
            }
        }).print();*/
        /*visitStream
                .map(new MapFunction<Visit, Tuple2<String, Visit>>() {
                    @Override
                    public Tuple2<String, Visit> map(Visit visit) throws Exception {
                        return Tuple2.of(visit.getDomain(), visit);
                    }
                })
                .partitionCustom(new MyPartitioner(), x -> x.f0)
                .map(new MapFunction<Tuple2<String, Visit>, Visit>() {
                    @Override
                    public Visit map(Tuple2<String, Visit> value) throws Exception {
                        System.out.println("current thread id :" + Thread.currentThread().getId() + ", value : " + value);
                        return value.f1;
                    }
                })
                .print();*/
        /**
         * 测输出流
         */
        // 分流操作
        OutputTag<Visit> bingOutputTag = new OutputTag<Visit>("bing");
        OutputTag<Visit> googleOutputTag = new OutputTag<Visit>("google");
        SingleOutputStreamOperator<Visit> processStream = visitStream.process(new ProcessFunction<Visit, Visit>() {
            @Override
            public void processElement(Visit visit, ProcessFunction<Visit, Visit>.Context context, Collector<Visit> collector) throws Exception {
                if ("bing.com".equals(visit.getDomain())) {
                    context.output(bingOutputTag, visit);
                } else if ("google.com".equals(visit.getDomain())) {
                    context.output(googleOutputTag, visit);
                } else {
                    collector.collect(visit);
                }
            }
        });

        DataStream<Visit> bingStream = processStream.getSideOutput(bingOutputTag);
        DataStream<Visit> googleStream = processStream.getSideOutput(googleOutputTag);

        bingStream.print("bing域名的流->");
        googleStream.print("google域名的流->");
        processStream.print("主流->");

        env.execute("Transformation Demo");
    }
}
