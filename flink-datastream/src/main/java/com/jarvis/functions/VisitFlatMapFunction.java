package com.jarvis.functions;

import com.jarvis.bean.Visit;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

public class VisitFlatMapFunction extends RichFlatMapFunction<Visit, String> {
    @Override
    public void flatMap(Visit visit, Collector<String> collector) throws Exception {
        if (visit.getDomain().equals("bing.com")) {
            collector.collect(visit.getDomain());
        } else {
            collector.collect("--domain--" + visit.getDomain());
            collector.collect("--traffic--" + visit.getTraffic());
        }
    }
}
