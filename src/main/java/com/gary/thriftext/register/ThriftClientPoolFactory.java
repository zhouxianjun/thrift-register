package com.gary.thriftext.register;

import com.gary.thriftext.register.invoker.Invoker;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool.BasePoolableObjectFactory;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.TServiceClientFactory;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.lang.reflect.Constructor;

/**
 * @author zhouxianjun(Gary)
 * @ClassName:
 * @Description: 连接池,thrift-client for spring
 * @date 2016/4/22 15:40
 */
@Slf4j
public class ThriftClientPoolFactory extends BasePoolableObjectFactory<TServiceClient> {
    private final TServiceClientFactory<TServiceClient> clientFactory;
    private Class<TTransport> transportClass;
    private Class<TProtocol> protocolClass;
    private PoolOperationCallBack callback;
    private Invoker invoker;

    private String serviceName;

    public ThriftClientPoolFactory(TServiceClientFactory<TServiceClient> clientFactory,
                                      Invoker invoker,
                                      Class<TTransport> transportClass,
                                      Class<TProtocol> protocolClass) {
        this.clientFactory = clientFactory;
        this.transportClass = transportClass;
        this.protocolClass = protocolClass;
        this.serviceName = invoker.getInterface().getEnclosingClass().getSimpleName();
        this.invoker = invoker;
    }

    public ThriftClientPoolFactory(TServiceClientFactory<TServiceClient> clientFactory,
                                      Invoker invoker,
                                      Class<TTransport> transportClass,
                                      Class<TProtocol> protocolClass,
                                      PoolOperationCallBack callback) {
        this.clientFactory = clientFactory;
        this.callback = callback;
        this.transportClass = transportClass;
        this.protocolClass = protocolClass;
        this.serviceName = invoker.getInterface().getEnclosingClass().getSimpleName();
        this.invoker = invoker;
    }

    @Override
    public TServiceClient makeObject() throws Exception {
        TSocket tsocket = new TSocket(invoker.getHost(), invoker.getPort());
        Constructor<TTransport> transportConstructor = Utils.getConstructorByParent(transportClass, TSocket.class);
        TTransport transport = transportConstructor.newInstance(tsocket);
        Constructor<TProtocol> protocolConstructor = Utils.getConstructorByParent(protocolClass, TTransport.class);
        TProtocol protocol = protocolConstructor.newInstance(transport);
        TMultiplexedProtocol mp = new TMultiplexedProtocol(protocol, serviceName);
        TServiceClient client = this.clientFactory.getClient(mp);
        transport.open();
        if (callback != null) {
            try {
                callback.make(client);
            } catch (Exception e) {
                log.warn("make callback client error", e);
            }
        }
        return client;
    }

    public void destroyObject(TServiceClient client) throws Exception {
        if (callback != null) {
            try {
                callback.destroy(client);
            } catch (Exception e) {
                log.warn("destroy callback client error", e);
            }
        }
        TTransport pin = client.getInputProtocol().getTransport();
        pin.close();
    }

    public boolean validateObject(TServiceClient client) {
        TTransport pin = client.getInputProtocol().getTransport();
        return pin.isOpen();
    }
}
