package com.aliware.tianchi.entity;

import com.aliware.tianchi.processor.Observer;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

public class Predictor implements Observer {

    //private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new NamedInternalThreadFactory("time-window", true));

    private static final int N = 100;

    private static final double a = 2;

    private static final double b = 0.08;

    private final long[] predictions = new long[N];

    private final double[] weights = new double[N];

    AtomicInteger counter = new AtomicInteger(0);

    public Predictor() {
        for (int i = 0; i < N; ++i) {
            predictions[i] = 1 + Math.round(5000 * Math.pow(2, (i - N) / 7D));
            //predictions[i] = i + 1;
        }
        System.out.println("predictions: " + Arrays.toString(predictions));
        Arrays.fill(weights, 1D / N);
    }

    public double getPrediction() {
        double prediction = 0;
        double weight = 0;
        for (int i = 0; i < N; ++i) {
            prediction += predictions[i] * weights[i];
            weight += weights[i];
        }
        //System.out.println("prediction: " + prediction + " weight: " + weight + " predic: " + (prediction / weight));
        double res = prediction / weight;
        return Double.isNaN(res) ? 1 : res;
        //return Math.max(prediction, 1) / Math.max(weight, 1);
    }

    public void update(double RTT) {
        double roundRTT = Math.max(Math.round(RTT), 1);
        double[] tempWeights = new double[N];
        for (int i = 0; i < N; ++i) {
            double l = predictions[i] > roundRTT ? 2 * roundRTT : Math.pow(predictions[i] - roundRTT, 2);
            //double l = predictions[i] > roundRTT ? 2 * roundRTT : roundRTT - predictions[i];
            //System.out.println(roundRTT + " " + l);
            tempWeights[i] = weights[i] * Math.exp(-a * l);
            //tempWeights[i] = weights[i] * Math.pow(1.1, -a * l);
        }
        double pool = 0;
        for (int i = 0; i < N; ++i) {
            pool += b * tempWeights[i];
        }
        double c = 1 - b, d = pool / N;
//        for (int i = 0; i < N; ++i) {
//            weights[i] = c * tempWeights[i] + d;
//        }
        synchronized (weights) {
            for (int i = 0; i < N; ++i) {
                weights[i] = c * tempWeights[i] + d;
            }
            //System.out.println(Arrays.toString(weights));
        }
    }

    @Override
    public void onSampleComplete(double rate, double deltaRate, double avgRTT) {
//        Arrays.fill(weights, 1D / N);
        if (counter.getAndIncrement() % 3 == 0) {
            synchronized (weights) {
                for (int i = 0; i < N; ++i) {
                    weights[i] = N / Math.exp(Math.abs(predictions[i] - avgRTT));
                }
            }
        }
        this.update(avgRTT);
    }
}
