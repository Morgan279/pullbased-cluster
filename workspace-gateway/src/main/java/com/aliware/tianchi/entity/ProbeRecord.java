package com.aliware.tianchi.entity;

public class ProbeRecord {

    public ProbeRecord(int bound, double rate) {
        this.bound = bound;
        this.rate = rate;
    }

    public void update(int bound, double computingRate) {
        this.bound = bound;
        this.rate = computingRate;
    }

    public int bound;

    public double rate;

}
