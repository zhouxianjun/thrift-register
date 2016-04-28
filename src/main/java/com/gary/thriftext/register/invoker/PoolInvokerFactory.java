package com.gary.thriftext.register.invoker;

import lombok.Setter;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;

/**
 * @author zhouxianjun(Gary)
 * @ClassName:
 * @Description:
 * @date 2016/4/28 16:37
 */
public class PoolInvokerFactory implements InvokerFactory {
    @Setter
    private Class<TTransport> transportClass;
    @Setter
    private Class<TProtocol> protocolClass;
    @Setter
    private int maxActive = 100;
    @Setter
    private int idleTime = 180000; // -1,关闭空闲检测

    public Invoker newInvoker(String address, Class<?> interfaceClass) throws Exception {
        return new PoolInvoker(address, interfaceClass, transportClass, protocolClass, maxActive, idleTime);
    }
}
