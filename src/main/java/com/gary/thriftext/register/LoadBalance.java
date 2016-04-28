package com.gary.thriftext.register;

import com.gary.thriftext.register.invoker.Invoker;

import java.lang.reflect.Method;
import java.util.List;

/**
 * @author zhouxianjun(Gary)
 * @ClassName:
 * @Description: 负载均衡方案
 * @date 2016/4/27 9:46
 */
public interface LoadBalance {
    Invoker selector(List<Invoker> addressList, Method method);
}
