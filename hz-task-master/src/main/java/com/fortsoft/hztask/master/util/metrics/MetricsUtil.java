package com.fortsoft.hztask.master.util.metrics;

import com.google.common.collect.Lists;
import com.fortsoft.hztask.master.metrics.CounterMetric;

import java.util.List;

/**
 * @author Serban Balamaci
 */
public class MetricsUtil {

    public static List<CounterMetric> getCounters(List metrics) {
        List<CounterMetric> counters = Lists.newArrayList();

        for(Object metric : metrics) {
            if(metric instanceof CounterMetric) {
                counters.add((CounterMetric)metric);
            }
        }
        return counters;
    }

}
