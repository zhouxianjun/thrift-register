package com.gary.thriftext.register.loadbalance;

import com.gary.thriftext.register.LoadBalance;
import com.gary.thriftext.register.invoker.Invoker;

import java.lang.reflect.Method;
import java.util.List;

/**
 * @author zhouxianjun(Gary)
 * @ClassName:
 * @Description:
 * @date 2016/4/27 9:48
 */
public abstract class AbstractLoadBalance implements LoadBalance {
    @Override
    public Invoker selector(List<Invoker> invokers, Method method) {
        if (invokers == null || invokers.isEmpty())
            return null;
        if (invokers.size() == 1)
            return invokers.get(0);
        return doSelect(invokers, method);
    }

    protected abstract Invoker doSelect(List<Invoker> invokers, Method method);

    /**
     * 获取权重
     * @param invoker
     * @return
     */
    protected int getWeight(Invoker invoker) {
        // 先获取provider配置的权重（默认100）
        int weight = invoker.getWeight();
        if (weight > 0) {
            long timestamp = invoker.getStartTime();
            if (timestamp > 0L) {
                // 计算出启动时长
                int uptime = (int) (System.currentTimeMillis() - timestamp);
                // 获取预热时间（默认600000，即10分钟）
                int warmup = invoker.getWarmup();
                // 如果启动时长小于预热时间，则需要降权。 权重计算方式为启动时长占预热时间的百分比乘以权重，
                // 如启动时长为20000ms，预热时间为60000ms，权重为120，则最终权重为 120 * （1/3) = 40，
                // 注意calculateWarmupWeight使用float进行计算，因此结果并不精确。
                if (uptime > 0 && uptime < warmup) {
                    weight = calculateWarmupWeight(uptime, warmup, weight);
                }
            }
        }
        return weight;
    }

    static int calculateWarmupWeight(int uptime, int warmup, int weight) {
        int ww = (int) ( (float) uptime / ( (float) warmup / (float) weight ) );
        return ww < 1 ? 1 : (ww > weight ? weight : ww);
    }
}
