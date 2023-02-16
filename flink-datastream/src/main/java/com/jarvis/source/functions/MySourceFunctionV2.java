package com.jarvis.source.functions;

import com.jarvis.bean.Visit;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * 自定义但并行度数据源
 */
public class MySourceFunctionV2 implements ParallelSourceFunction<Visit> {

    private volatile boolean isRunning = true;

    @Override
    public void run(SourceContext<Visit> ctx) throws Exception {
        Random random = new Random();
        String[] domains = {"baidu.com", "google.com", "bing.com", "youtube.com"};
        while (isRunning) {
            long ts = System.currentTimeMillis();
            ctx.collect(new Visit(ts, domains[random.nextInt(domains.length)], random.nextInt(1000) + 1000));
            Thread.sleep(2000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
