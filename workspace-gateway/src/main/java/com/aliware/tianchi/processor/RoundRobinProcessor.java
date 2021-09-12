package com.aliware.tianchi.processor;

import java.util.HashMap;
import java.util.Map;

public class RoundRobinProcessor {

    private static final Map<Integer, Integer> currentWeightMap = new HashMap<>();

    public static void register(int port) {
        currentWeightMap.putIfAbsent(port, 0);
    }


    public static int select(Map<Integer, Integer> weightMap) {
        int totalWeight = 0;
        int maxWeight = -1;
        int maxWeightPort = -1;

        synchronized (currentWeightMap) {
            for (Integer port : weightMap.keySet()) {
                int weight = weightMap.get(port);
                totalWeight += weight;
                int currentWeight = currentWeightMap.get(port) + weight;
                currentWeightMap.put(port, currentWeight);
                if (currentWeight > maxWeight) {
                    maxWeight = currentWeight;
                    maxWeightPort = port;
                }
            }
            currentWeightMap.put(maxWeightPort, currentWeightMap.get(maxWeightPort) - totalWeight);
        }
        
        return maxWeightPort;
    }
}
