package com.jarvis.functions;

import com.jarvis.bean.Visit;
import org.apache.flink.api.common.functions.RichFilterFunction;

public class VisitFilterFunction extends RichFilterFunction<Visit> {
    @Override
    public boolean filter(Visit visit) throws Exception {
        return visit.getTraffic() > 4000;
    }
}
