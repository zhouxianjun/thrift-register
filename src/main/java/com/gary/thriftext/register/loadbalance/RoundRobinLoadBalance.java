package com.gary.thriftext.register.loadbalance;

import com.gary.thriftext.register.dto.AtomicPositiveInteger;
import com.gary.thriftext.register.invoker.Invoker;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author zhouxianjun(Gary)
 * @ClassName:
 * @Description: 轮询调度
 * @date 2016/4/27 11:12
 */
public class RoundRobinLoadBalance extends AbstractLoadBalance {

    private final ConcurrentMap<String, AtomicPositiveInteger> sequences = new ConcurrentHashMap<>();

    private final ConcurrentMap<String, AtomicPositiveInteger> weightSequences = new ConcurrentHashMap<>();

    @Override
    protected Invoker doSelect(List<Invoker> invokers, Method method) {
        String key = invokers.get(0).getInterface().getName() + ":" + method.getName();
        int length = invokers.size(); // 总个数
        int maxWeight = 0; // 最大权重
        int minWeight = Integer.MAX_VALUE; // 最小权重
        for (int i = 0; i < length; i++) {
            int weight = getWeight(invokers.get(i));
            maxWeight = Math.max(maxWeight, weight); // 累计最大权重
            minWeight = Math.min(minWeight, weight); // 累计最小权重
        }
        if (maxWeight > 0 && minWeight < maxWeight) { // 权重不一样
            AtomicPositiveInteger weightSequence = weightSequences.get(key);
            if (weightSequence == null) {
                weightSequences.putIfAbsent(key, new AtomicPositiveInteger());
                weightSequence = weightSequences.get(key);
            }
            int currentWeight = weightSequence.getAndIncrement() % maxWeight;
            List<Invoker> weightInvokers = new ArrayList<>();
            for (Invoker invoker : invokers) { // 筛选权重大于当前权重基数的Invoker
                if (getWeight(invoker) > currentWeight) {
                    weightInvokers.add(invoker);
                }
            }
            int weightLength = weightInvokers.size();
            if (weightLength == 1) {
                return weightInvokers.get(0);
            } else if (weightLength > 1) {
                invokers = weightInvokers;
                length = invokers.size();
            }
        }
        AtomicPositiveInteger sequence = sequences.get(key);
        if (sequence == null) {
            sequences.putIfAbsent(key, new AtomicPositiveInteger());
            sequence = sequences.get(key);
        }
        // 取模轮循
        return invokers.get(sequence.getAndIncrement() % length);
    }
}
