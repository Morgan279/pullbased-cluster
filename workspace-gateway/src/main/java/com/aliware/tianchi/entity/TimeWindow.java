package com.aliware.tianchi.entity;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

public class TimeWindow {

    private final int WINDOW_SIZE = 11;

    private final Deque<Double> deque = new ArrayDeque<>(WINDOW_SIZE + 1);

    private final AtomicInteger counter = new AtomicInteger(0);

    //private Map<Double, Integer> indexMap = new ConcurrentHashMap<>();

    public void addNewSample(double RTT) {
        //indexMap.put(RTT, counter.getAndIncrement());
        counter.getAndIncrement();
        synchronized (deque) {
            while (!deque.isEmpty() && deque.peekLast() < RTT) {
                deque.pollLast();
            }
            if (counter.get() % WINDOW_SIZE == 0) {
                deque.pollFirst();
            }
            deque.addLast(RTT);
        }
    }

    public Double getMaxRTT() {
        return Optional.ofNullable(deque.peekFirst()).orElse(10D);
    }
}
