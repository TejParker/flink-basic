package com.jarvis.functions;

import com.jarvis.bean.Visit;
import org.apache.flink.api.common.functions.RichMapFunction;

public class VisitConvertFunction extends RichMapFunction<String, Visit> {
    @Override
    public Visit map(String s) throws Exception {
        String[] split = s.split(",");
        return new Visit(Long.parseLong(split[0].trim()), split[1].trim(),Double.parseDouble(split[2].trim()));
    }
}
