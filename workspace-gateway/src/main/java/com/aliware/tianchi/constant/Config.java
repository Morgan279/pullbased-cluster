package com.aliware.tianchi.constant;

public abstract class Config {

    public static final int SAMPLING_COUNT = 10;

    public static final int INITIAL_AVERAGE_RTT = 1;

    public static final int CONGESTION_SCAN_INTERVAL = 100;

    public static final int DRAIN_INTERVAL = 4;

    public static final double DRAIN_GAIN = 0.1;

    public static final int INFLIGHT_BOUND_FACTOR = 32;

    public static final int RT_PROP_ESTIMATE_VALUE = 44;

    public static final int RT_TIME_WINDOW = 6;

    public static final int COMPUTING_RATE_WINDOW_FACTOR = 6;

    public static final double[] GAIN_VALUES = {1.01, 0.99, 1, 1, 1, 1, 1, 1};
}
