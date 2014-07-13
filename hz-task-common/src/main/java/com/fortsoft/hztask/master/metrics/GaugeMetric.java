package com.fortsoft.hztask.master.metrics;

/**
 * @author Serban Balamaci
 */
public class GaugeMetric extends BaseMetric {

    private Object value;

    public GaugeMetric(String name, Object value) {
        super(name);
        this.value = value;
    }
}
