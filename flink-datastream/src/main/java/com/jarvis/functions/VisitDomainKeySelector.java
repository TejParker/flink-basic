package com.jarvis.functions;

import com.jarvis.bean.Visit;
import org.apache.flink.api.java.functions.KeySelector;

public class VisitDomainKeySelector implements KeySelector<Visit, String> {
    @Override
    public String getKey(Visit visit) throws Exception {
        return visit.getDomain();
    }
}
