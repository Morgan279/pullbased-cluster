package com.aliware.tianchi.entity;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class TimeWindow {

    private final int WINDOW_SIZE = 50;

    private final Deque<Double> deque = new ArrayDeque<>(WINDOW_SIZE);

    private AtomicInteger counter = new AtomicInteger(0);

    private Map<Double, Integer> indexMap = new ConcurrentHashMap<>();

    public void addNewSample(double RTT) {
        indexMap.put(RTT, counter.getAndIncrement());
        synchronized (deque) {
            while (!deque.isEmpty() && deque.peekLast() < RTT) {
                deque.pollLast();
            }
            deque.add(RTT);
            if (counter.get() - indexMap.get(deque.peekFirst()) >= WINDOW_SIZE) {
                deque.pollFirst();
            }
        }
    }

    public Double getMaxRTT() {
        return deque.peekFirst();
    }
}
