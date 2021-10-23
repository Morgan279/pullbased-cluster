package com.aliware.tianchi.entity;

import java.util.Arrays;

public class Predictor {

    private static final int N = 5000;

    private static final double a = 2;

    private static final double b = 1;

    private final int[] predictions = new int[N];

    private final double[] weights = new double[N];

    public Predictor() {
        for (int i = 0; i < N; ++i) {
            predictions[i] = i + 1;
        }
        Arrays.fill(weights, 1D / N);
    }

    public double getPrediction() {
        double prediction = 0;
        double weight = 0;
        for (int i = 0; i < N; ++i) {
            prediction += predictions[i] * weights[i];
            weight += weights[i];
        }
        return Math.max(prediction, 1) / Math.max(weight, 1);
    }

    public void update(double RTT) {
        double[] tempWeights = new double[N];
        for (int i = 0; i < N; ++i) {
            double l = predictions[i] < RTT ? 2 * RTT : Math.pow(predictions[i] - RTT, 2);
            tempWeights[i] = weights[i] * Math.exp(-a * l);
        }
        double pool = 0;
        for (int i = 0; i < N; ++i) {
            pool += b * tempWeights[i];
        }
        double c = 1 - b, d = pool / N;
        for (int i = 0; i < N; ++i) {
            weights[i] = c * tempWeights[i] + d;
        }
        synchronized (weights) {
            for (int i = 0; i < N; ++i) {
                weights[i] = c * tempWeights[i] + d;
            }
        }
    }
}
