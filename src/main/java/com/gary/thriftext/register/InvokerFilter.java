package com.gary.thriftext.register;

import java.lang.reflect.Method;

/**
 * @author zhouxianjun(Gary)
 * @ClassName:
 * @Description:
 * @date 2016/4/27 11:54
 */
public interface InvokerFilter {
    boolean before(Object proxy, Method method, Object[] args);

    void after(Object proxy, long time, boolean success);
}
