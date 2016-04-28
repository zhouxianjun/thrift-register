package com.gary.thriftext.register.invoker;

import java.lang.reflect.Method;

/**
 * @author zhouxianjun(Gary)
 * @ClassName:
 * @Description:
 * @date 2016/4/27 9:57
 */
public interface Invoker {

    String getAddress();

    String getHost();

    int getPort();

    int getWeight();

    long getStartTime();

    int getWarmup();

    Class<?> getInterface();

    Object invoker(Method method, Object...args) throws Exception;

    boolean isAvailable();

    void destroy();
}
