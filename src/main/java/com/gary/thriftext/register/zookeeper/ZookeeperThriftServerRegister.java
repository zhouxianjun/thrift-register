package com.gary.thriftext.register.zookeeper;

import com.gary.thriftext.register.ThriftServerRegister;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.InitializingBean;

import java.io.UnsupportedEncodingException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * @author zhouxianjun(Gary)
 * @ClassName:
 * @Description: 注册服务列表到Zookeeper 注册中心
 * @date 16-4-23 上午1:01
 */
@Slf4j
public class ZookeeperThriftServerRegister implements ThriftServerRegister, InitializingBean {
    @Setter
    private CuratorFramework zkClient;

    private Set<String> services = new HashSet<>();

    @Override
    public void register(String service, String version, String address) throws Exception {
        String path = "/" + service + "/" + version + "/" + address;
        reg(path);
    }

    private void reg(String path) throws Exception {
        //临时节点
        try {
            Stat stat = zkClient.checkExists().forPath(path);
            if (stat != null) {
                return;
            }
            zkClient.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.EPHEMERAL)
                    .forPath(path);
            services.add(path);
            log.info("register path:{} for zookeeper.", path);
        } catch (UnsupportedEncodingException e) {
            log.error("register service address to zookeeper exception", e);
            throw new Exception("register service address to zookeeper exception: address UnsupportedEncodingException", e);
        } catch (Exception e) {
            log.error("register service address to zookeeper exception", e);
            throw new Exception("register service address to zookeeper exception", e);
        }
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        if(zkClient.getState() == CuratorFrameworkState.LATENT){
            zkClient.start();
        }
        zkClient.getConnectionStateListenable().addListener(new ConnectionStateListener() {
            @Override
            public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
                if (connectionState == ConnectionState.RECONNECTED && !services.isEmpty()) {
                    try {
                        log.info("sleep 5 ms rebuild.");
                        Thread.sleep(1000 * 5);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    for (String next : services) {
                        try {
                            reg(next);
                        } catch (Exception e) {
                            log.warn("reregister for {} error", next, e);
                        }
                    }
                }
            }
        });
    }
}
