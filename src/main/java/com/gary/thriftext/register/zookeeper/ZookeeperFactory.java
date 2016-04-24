package com.gary.thriftext.register.zookeeper;

import lombok.Setter;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.util.StringUtils;

import java.io.Closeable;

/**
 * @author zhouxianjun(Gary)
 * @ClassName:
 * @Description: 获取zookeeper客户端链接
 * @date 2016/4/22 11:45
 */
public class ZookeeperFactory implements FactoryBean<CuratorFramework>, Closeable {
    @Setter
    private String hosts;
    // session超时
    @Setter
    private int sessionTimeout = 30000;
    @Setter
    private int connectionTimeout = 30000;

    // 共享一个zk链接
    @Setter
    private boolean singleton = true;

    // 全局path前缀,常用来区分不同的应用
    @Setter
    private String namespace = "demo";

    public final static String ROOT = "rpc";

    private CuratorFramework zkClient;

    @Override
    public CuratorFramework getObject() throws Exception {
        if (singleton) {
            if (zkClient == null) {
                zkClient = create();
                zkClient.start();
            }
            return zkClient;
        }
        return create();
    }

    @Override
    public Class<?> getObjectType() {
        return CuratorFramework.class;
    }

    @Override
    public boolean isSingleton() {
        return singleton;
    }

    public CuratorFramework create() throws Exception {
        if (StringUtils.isEmpty(namespace)) {
            namespace = ROOT;
        } else {
            namespace = ROOT +"/"+ namespace;
        }
        return create(hosts, sessionTimeout, connectionTimeout, namespace);
    }

    public static CuratorFramework create(String connectString, int sessionTimeout, int connectionTimeout, String namespace) {
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
        return builder.connectString(connectString).sessionTimeoutMs(sessionTimeout).connectionTimeoutMs(connectionTimeout)
                .canBeReadOnly(true).namespace(namespace).retryPolicy(new ExponentialBackoffRetry(1000, Integer.MAX_VALUE))
                .defaultData(null).build();
    }

    public void close() {
        if (zkClient != null) {
            zkClient.close();
        }
    }
}
