package com.gary.thriftext.register.invoker;

/**
 * @author zhouxianjun(Gary)
 * @ClassName:
 * @Description:
 * @date 2016/4/28 16:39
 */
public interface InvokerFactory {
    Invoker newInvoker(String address, Class<?> interfaceClass) throws Exception;
}
