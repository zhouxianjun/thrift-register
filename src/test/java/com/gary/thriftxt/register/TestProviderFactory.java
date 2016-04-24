package com.gary.thriftxt.register;

import com.gary.thriftext.register.zookeeper.ZookeeperFactory;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.cache.*;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

/**
 * @author zhouxianjun(Gary)
 * @ClassName:
 * @Description:
 * @date 2016/4/22 17:02
 */
@Component
@Slf4j
public class TestProviderFactory implements InitializingBean {
    @Autowired
    private CuratorFramework zkClient;

    private TreeCache treeCache;
    private ConcurrentHashMap<String, List<InetSocketAddress>> cacheMap = new ConcurrentHashMap<>();
    private CountDownLatch countDownLatch = new CountDownLatch(1);
    @Override
    public void afterPropertiesSet() throws Exception {
        // 如果zk尚未启动,则启动
        if (zkClient.getState() == CuratorFrameworkState.LATENT) {
            zkClient.start();
        }
        buildAllServicePathCache(zkClient, "/", true);
        treeCache.start();
        //countDownLatch.await();
    }

    private List<InetSocketAddress> transfer(String address) {
        String[] hostname = address.split(":");
        Integer weight = 1;
        if (hostname.length == 3) {
            weight = Integer.valueOf(hostname[2]);
        }
        String ip = hostname[0];
        Integer port = Integer.valueOf(hostname[1]);
        List<InetSocketAddress> result = new ArrayList<>();
        // 根据优先级，将ip：port添加多次到地址集中，然后随机取地址实现负载
        for (int i = 0; i < weight; i++) {
            result.add(new InetSocketAddress(ip, port));
        }
        return result;
    }

    private void buildAllServicePathCache(final CuratorFramework client, String path, Boolean cacheData) {
        treeCache = new TreeCache(client, path);
        treeCache.getListenable().addListener(new TreeCacheListener() {
            @Override
            public void childEvent(CuratorFramework curatorFramework, TreeCacheEvent treeCacheEvent) throws Exception {
                TreeCacheEvent.Type type = treeCacheEvent.getType();
                log.info("zookeeper event:{}", type);
                cacheMap.clear();
                rebuild("/");

                for (Map.Entry<String, List<InetSocketAddress>> entry : cacheMap.entrySet()) {
                    log.info("{}-{}", entry.getKey(), Arrays.toString(entry.getValue().toArray()));
                }
            }

            private void rebuild(String root) {
                Map<String, ChildData> currentChildren = treeCache.getCurrentChildren(root);
                if (currentChildren != null) {
                    for (Map.Entry<String, ChildData> entry : currentChildren.entrySet()) {
                        String path = entry.getValue().getPath();
                        log.info("zookeeper rebuild path:{}", path);
                        String[] array = path.split("/");
                        if (array.length == 4) {
                            String key = array[1] + ":" + array[2];
                            cacheMap.putIfAbsent(key, new ArrayList<InetSocketAddress>());
                            cacheMap.get(key).addAll(transfer(array[3]));
                        }
                        rebuild(path);
                    }
                }
            }
        });
    }
}
