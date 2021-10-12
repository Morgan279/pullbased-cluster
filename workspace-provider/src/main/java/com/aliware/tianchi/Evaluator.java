package com.aliware.tianchi;

import com.aliware.tianchi.tool.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class Evaluator {

    private final static Logger LOGGER = LoggerFactory.getLogger(Evaluator.class);

    private static final int N = 32;

    private static final double AVG = (N + 1) / 2D;

    private final int[] bounds = new int[N];

    private final double[] rates = new double[N];

    private int counter = 0;

    private int maxBound = 0;

    private volatile double evaluate = 0;

    private final StopWatch stopWatch = new StopWatch();

    public synchronized void addSample(int bound, double rate) {
        bounds[counter] = bound;
        rates[counter++] = rate;
        maxBound = Math.max(maxBound, bound);
        if (counter == N) {
            counter = 0;
            this.refreshEvaluate();
        }
    }

    public double getEvaluate() {
        return evaluate + 1D;
    }

    private void refreshEvaluate() {
//        double interval = stopWatch.stop();
//        LOGGER.info("interval: {} rs: {}", interval, evaluate);
        int[] boundBucket = new int[maxBound + 1];
        Arrays.fill(boundBucket, 0);
        for (int i = 0; i < N; ++i) {
            ++boundBucket[bounds[i]];
        }

        double[] boundRanks = new double[maxBound + 1];
        //       StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0, rank = 1; i <= maxBound; ++i) {
            if (boundBucket[i] > 0) {
                if (boundBucket[i] == N) {
                    this.evaluate = this.maxBound = 0;
                    return;
                }
                boundRanks[i] = rank + (boundBucket[i] - 1) / 2D;
                rank += boundBucket[i];
//                stringBuilder.append(i).append("-").append(boundBucket[i]).append(':').append(boundRanks[i]).append("  ");
            }
        }
//        LOGGER.info("bound rank: {}", stringBuilder.toString());

        Map<Double, Integer> rateRanks = new HashMap<>(N);
        double[] tempRates = Arrays.copyOf(rates, N);
        Arrays.sort(tempRates);
        for (int i = 0; i < N; ++i) {
            rateRanks.put(tempRates[i], i + 1);
        }

        double num = 0, denom1 = 0, denom2 = 0;
        for (int i = 0; i < N; ++i) {
            double x = boundRanks[bounds[i]] - AVG;
            double y = rateRanks.get(rates[i]) - AVG;
            num += x * y;
            denom1 += x * x;
            denom2 += y * y;
        }
        this.evaluate = num / (Math.sqrt(denom1) * Math.sqrt(denom2));
        this.maxBound = 0;
//        stopWatch.start();
    }

}
