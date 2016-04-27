package com.gary.thriftext.register.loadbalance;

import com.gary.thriftext.register.dto.Invoker;

import java.util.List;
import java.util.Random;

/**
 * @author zhouxianjun(Gary)
 * @ClassName:
 * @Description: 随机选举
 * @date 2016/4/27 10:49
 */
public class RandomLoadBalance extends AbstractLoadBalance {

    private final Random random = new Random();

    @Override
    protected Invoker doSelect(List<Invoker> invokers) {
        int length = invokers.size(); // 总个数
        int totalWeight = 0; // 总权重
        boolean sameWeight = true; // 权重是否都一样
        for (int i = 0; i < length; i++) {
            int weight = getWeight(invokers.get(i));
            totalWeight += weight; // 累计总权重
            if (sameWeight && i > 0 && weight != getWeight(invokers.get(i - 1))) {
                sameWeight = false; // 计算所有权重是否一样
            }
        }
        if (totalWeight > 0 && !sameWeight) {
            // 如果权重不相同且权重大于0则按总权重数随机
            int offset = random.nextInt(totalWeight);
            // 并确定随机值落在哪个片断上
            for (int i = 0; i < length; i++) {
                offset -= getWeight(invokers.get(i));
                if (offset < 0) {
                    return invokers.get(i);
                }
            }
        }
        // 如果权重相同或权重为0则均等随机
        return invokers.get(random.nextInt(length));
    }
}
