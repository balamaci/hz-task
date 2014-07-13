package com.fortsoft.hztask.master.metrics;

import java.io.Serializable;

/**
 * @author Serban Balamaci
 */
public class BaseMetric implements Serializable {

    private String name;

    public BaseMetric(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
