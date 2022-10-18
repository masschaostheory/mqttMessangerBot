package com.masschaostheory.entity;

import java.util.ArrayList;
import java.util.List;

public class MqttSensor {

    private final String id;
    private List<Measures> measures = new ArrayList<>();

    public MqttSensor(String id) {
        this.id = id;
    }

    public List<Measures> getMeasures() {
        return measures;
    }

    public void setMeasures(List<Measures> measures) {
        this.measures = measures;
    }

    public void clearMeasures() {
        measures = new ArrayList<>();
    }

    public void addMeasure(String dis, String unit, float val)
    {
        measures.add(new Measures(dis, unit, val));
    }

    public String getId() {
        return id;
    }

}
