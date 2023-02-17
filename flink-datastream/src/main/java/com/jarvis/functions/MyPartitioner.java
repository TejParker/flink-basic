package com.jarvis.functions;

import org.apache.flink.api.common.functions.Partitioner;

public class MyPartitioner implements Partitioner<String> {
    @Override
    public int partition(String key, int numPartitions) {
        System.out.println("numPartitions = " + numPartitions);
        if ("bing.com".equals(key)) {
            return 0;
        } else if ("google.com".equals(key)) {
            return 1;
        } else {
            return 2;
        }
    }
}
