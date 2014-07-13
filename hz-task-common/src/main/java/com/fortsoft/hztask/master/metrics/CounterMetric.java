package com.fortsoft.hztask.master.metrics;

/**
 * @author Serban Balamaci
 */
public class CounterMetric extends BaseMetric {

    private final long value;

    public CounterMetric(String name, long value) {
        super(name);
        this.value = value;
    }

    public long getValue() {
        return value;
    }
}
