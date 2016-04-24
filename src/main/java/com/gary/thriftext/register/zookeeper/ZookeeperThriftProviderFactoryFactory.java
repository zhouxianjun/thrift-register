package com.gary.thriftext.register.zookeeper;

import com.gary.thriftext.register.ThriftServerProviderFactory;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.springframework.beans.factory.InitializingBean;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

/**
 * @author zhouxianjun(Gary)
 * @ClassName:
 * @Description: 使用zookeeper作为"config"中心,使用apache-curator方法库来简化zookeeper开发
 * @date 2016/4/22 11:56
 */
@Slf4j
@NoArgsConstructor
public class ZookeeperThriftProviderFactoryFactory implements InitializingBean, ThriftServerProviderFactory {
    @Setter
    private CuratorFramework zkClient;

    private TreeCache treeCache;

    private CountDownLatch countDownLatch = new CountDownLatch(1);

    private ConcurrentHashMap<String, Queue<InetSocketAddress>> queueMap = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, List<InetSocketAddress>> cacheMap = new ConcurrentHashMap<>();

    private final Object lock = new Object();
    // 默认权重
    private static final Integer DEFAULT_WEIGHT = 1;

    public ZookeeperThriftProviderFactoryFactory(CuratorFramework zkClient) {
        this.zkClient = zkClient;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        // 如果zk尚未启动,则启动
        if (zkClient.getState() == CuratorFrameworkState.LATENT) {
            zkClient.start();
        }
        listenTree();
        treeCache.start();
        countDownLatch.await();
    }

    private void listenTree() {
        treeCache = new TreeCache(zkClient, "/");
        treeCache.getListenable().addListener(new TreeCacheListener() {
            @Override
            public void childEvent(CuratorFramework curatorFramework, TreeCacheEvent treeCacheEvent) throws Exception {
                TreeCacheEvent.Type type = treeCacheEvent.getType();
                log.debug("zookeeper event:{}", type);
                cacheMap.clear();
                rebuild("/");
                for (Map.Entry<String, List<InetSocketAddress>> entry : cacheMap.entrySet()) {
                    log.info("zookeeper subscribe {}-{}", entry.getKey(), Arrays.toString(entry.getValue().toArray()));
                    Collections.shuffle(entry.getValue());
                    synchronized (lock) {
                        queueMap.clear();
                        queueMap.putIfAbsent(entry.getKey(), new LinkedList<InetSocketAddress>());
                        queueMap.get(entry.getKey()).addAll(entry.getValue());
                    }
                }
                if (type == TreeCacheEvent.Type.INITIALIZED)
                    countDownLatch.countDown();
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


    private List<InetSocketAddress> transfer(String address) {
        String[] hostname = address.split(":");
        Integer weight = DEFAULT_WEIGHT;
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

    /**
     * 获取所有服务端地址
     * @param service 接口
     * @param version 版本
     * @return
     */
    public List<InetSocketAddress> findServerAddressList(String service, String version) {
        String key = service + ":" + version;
        List<InetSocketAddress> addresses = cacheMap.get(key);
        return addresses == null ? null : Collections.unmodifiableList(addresses);
    }

    /**
     * 选取一个合适的address,可以随机获取等'
     * 使用权重算法.
     * @param service 接口
     * @param version 版本
     * @return
     */
    public synchronized InetSocketAddress selector(String service, String version) {
        String key = service + ":" + version;
        Queue<InetSocketAddress> queue = queueMap.get(service);

        if (queue == null || queue.isEmpty()) {
            List<InetSocketAddress> addresses = cacheMap.get(key);
            if (addresses != null && !addresses.isEmpty()) {
                queueMap.putIfAbsent(key, new LinkedList<InetSocketAddress>());
                queueMap.get(key).addAll(addresses);
            }
        }

        queue = queueMap.get(key);
        return queue == null ? null : queue.poll();
    }

    public void close() {
        try {
            treeCache.close();
            zkClient.close();
        } catch (Exception e) {
            log.warn("stop TreeCache or zkClient error", e);
        }
    }
}
