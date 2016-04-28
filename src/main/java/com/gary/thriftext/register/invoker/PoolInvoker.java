package com.gary.thriftext.register.invoker;

import com.gary.thriftext.register.ThriftClientPoolFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.TServiceClientFactory;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;
import org.springframework.util.ClassUtils;

import java.lang.reflect.Method;

/**
 * @author zhouxianjun(Gary)
 * @ClassName:
 * @Description:
 * @date 2016/4/28 15:23
 */
@Slf4j
public class PoolInvoker extends AbstractInvoker {
    private GenericObjectPool<TServiceClient> pool;

    public PoolInvoker(String address, Class<?> interfaceClass,
                       Class<TTransport> transportClass, Class<TProtocol> protocolClass,
                       int maxActive, int idleTime) throws Exception {
        super(address, interfaceClass);

        // 加载Iface接口
        final String name = getInterface().getName();
        // 加载Client.Factory类
        Class<TServiceClientFactory<TServiceClient>> fi = (Class<TServiceClientFactory<TServiceClient>>) ClassUtils.forName(name.replace("$Iface", "") + "$Client$Factory", null);
        TServiceClientFactory<TServiceClient> clientFactory = fi.newInstance();
        ThriftClientPoolFactory poolFactory = new ThriftClientPoolFactory(clientFactory, this, transportClass, protocolClass);
        GenericObjectPool.Config poolConfig = new GenericObjectPool.Config();
        poolConfig.maxActive = maxActive;
        poolConfig.minIdle = 0;
        poolConfig.minEvictableIdleTimeMillis = idleTime;
        poolConfig.timeBetweenEvictionRunsMillis = idleTime / 2L;

        pool = new GenericObjectPool<>(poolFactory, poolConfig);
    }

    @Override
    public Object invoker(Method method, Object... args) throws Exception {
        log.debug("Invoker Pool info: [active={}, idle={}]", pool.getNumActive(), pool.getNumIdle());
        TServiceClient client = pool.borrowObject();
        Object result = null;
        try {
            result = method.invoke(client, args);
        } finally {
            pool.returnObject(client);
        }
        return result;
    }

    @Override
    public boolean isAvailable() {
        return true;
    }

    @Override
    public void destroy() {
        try {
            pool.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
