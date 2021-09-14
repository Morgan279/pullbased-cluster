package com.aliware.tianchi.processor;

import java.util.function.Function;

public class ConcurrentLimitProcessor {

    private int initialLimit = 20;
    private double estimatedLimit = 20;
    private int maxConcurrency = 1000;
    private double smoothing = 1.0;

    private Function<Integer, Integer> LOG10 = v -> (int) Math.log10(v);

    private Function<Integer, Integer> alphaFunc = (limit) -> 3 * LOG10.apply(limit);
    private Function<Integer, Integer> betaFunc = (limit) -> 6 * LOG10.apply(limit);
    private Function<Integer, Integer> thresholdFunc = (limit) -> LOG10.apply(limit);
    private Function<Double, Double> increaseFunc = (limit) -> limit + LOG10.apply(limit.intValue());
    private Function<Double, Double> decreaseFunc = (limit) -> limit - LOG10.apply(limit.intValue());

    private int updateEstimatedLimit(long rtt, long rtt_noload, int inflight, boolean didDrop) {
        final int queueSize = (int) Math.ceil(estimatedLimit * (1 - (double) rtt_noload / rtt));

        double newLimit;
        // Treat any drop (i.e timeout) as needing to reduce the limit
        // 发现错误直接应用减函数decreaseFunc
        if (didDrop) {
            newLimit = decreaseFunc.apply(estimatedLimit);
            // Prevent upward drift if not close to the limit
        } else if (inflight * 2 < estimatedLimit) {
            return (int) estimatedLimit;
        } else {
            int alpha = alphaFunc.apply((int) estimatedLimit);
            int beta = betaFunc.apply((int) estimatedLimit);
            int threshold = this.thresholdFunc.apply((int) estimatedLimit);

            // Aggressive increase when no queuing
            if (queueSize <= threshold) {
                newLimit = estimatedLimit + beta;
                // Increase the limit if queue is still manageable
            } else if (queueSize < alpha) {
                newLimit = increaseFunc.apply(estimatedLimit);
                // Detecting latency so decrease
            } else if (queueSize > beta) {
                newLimit = decreaseFunc.apply(estimatedLimit);
                // We're within he sweet spot so nothing to do
            } else {
                return (int) estimatedLimit;
            }
        }

        newLimit = Math.max(1, newLimit);
        newLimit = (1 - smoothing) * estimatedLimit + smoothing * newLimit;
        estimatedLimit = newLimit;
        return (int) estimatedLimit;
    }

}
