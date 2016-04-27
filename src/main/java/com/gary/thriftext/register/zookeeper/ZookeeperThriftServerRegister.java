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
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;

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

    private Timer reRegTimer;

    // 休息多少毫秒重新注册(注册中心快速启动问题)
    @Setter
    private long sleepReg = 5000;

    private AtomicBoolean isRun = new AtomicBoolean(false);

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
                    if (reRegTimer != null && !isRun.get()) {
                        reRegTimer.cancel();
                        reRegTimer = null;
                    }
                    if (reRegTimer == null) {
                        reRegTimer = new Timer();
                        reRegTimer.schedule(new TimerTask() {
                            @Override
                            public void run() {
                                isRun.compareAndSet(false, true);
                                try {
                                    for (String next : services) {
                                        try {
                                            reg(next);
                                        } catch (Exception e) {
                                            log.warn("reregister for {} error", next, e);
                                        }
                                    }
                                    this.cancel();
                                } catch (Exception e) {
                                    log.warn("reregister task error", e);
                                } finally {
                                    reRegTimer = null;
                                    isRun.compareAndSet(true, false);
                                }
                            }
                        }, sleepReg);
                    }
                }
            }
        });
    }
}
