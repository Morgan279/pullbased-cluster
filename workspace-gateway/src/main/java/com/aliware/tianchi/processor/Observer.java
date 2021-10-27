package com.aliware.tianchi.processor;

public interface Observer {

    void onSampleComplete(double rate, double deltaRate, double avgRTT);

}
