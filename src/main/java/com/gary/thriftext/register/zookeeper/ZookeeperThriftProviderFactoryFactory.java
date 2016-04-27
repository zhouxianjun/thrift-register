package com.gary.thriftext.register.zookeeper;

import com.gary.thriftext.register.LoadBalance;
import com.gary.thriftext.register.ThriftServerProviderFactory;
import com.gary.thriftext.register.dto.Invoker;
import com.gary.thriftext.register.loadbalance.RandomLoadBalance;
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

    @Setter
    private LoadBalance loadBalance = new RandomLoadBalance();

    private TreeCache treeCache;

    private CountDownLatch countDownLatch = new CountDownLatch(1);

    private ConcurrentHashMap<String, List<Invoker>> cacheMap = new ConcurrentHashMap<>();

    private final Object lock = new Object();

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
                Map<String, List<Invoker>> temp = new HashMap<>();
                rebuild("/", temp);
                for (Map.Entry<String, List<Invoker>> entry : temp.entrySet()) {
                    log.info("zookeeper subscribe {}-{}", entry.getKey(), Arrays.toString(entry.getValue().toArray()));
                    Collections.shuffle(entry.getValue());
                    synchronized (lock) {
                        cacheMap.clear();
                        cacheMap.putIfAbsent(entry.getKey(), new ArrayList<Invoker>());
                        cacheMap.get(entry.getKey()).addAll(entry.getValue());
                    }
                }
                temp = null;
                if (type == TreeCacheEvent.Type.INITIALIZED)
                    countDownLatch.countDown();
            }

            private void rebuild(String root, Map<String, List<Invoker>> map) {
                Map<String, ChildData> currentChildren = treeCache.getCurrentChildren(root);
                if (currentChildren != null) {
                    for (Map.Entry<String, ChildData> entry : currentChildren.entrySet()) {
                        String path = entry.getValue().getPath();
                        log.info("zookeeper rebuild path:{}", path);
                        String[] array = path.split("/");
                        if (array.length == 4) {
                            String key = array[1] + ":" + array[2];
                            if (!map.containsKey(key))
                                map.put(key, new ArrayList<Invoker>());
                            map.get(key).add(new Invoker(array[3]));
                        }
                        rebuild(path, map);
                    }
                }
            }
        });
    }

    /**
     * 获取所有服务端地址
     * @param service 接口
     * @param version 版本
     * @return
     */
    public List<Invoker> findServerAddressList(String service, String version) {
        String key = service + ":" + version;
        List<Invoker> addresses = cacheMap.get(key);
        return addresses == null ? null : Collections.unmodifiableList(addresses);
    }

    /**
     * 选取一个合适的address,可以随机获取等'
     * 使用权重算法.
     * @param service 接口
     * @param version 版本
     * @return
     */
    public Invoker selector(String service, String version) {
        String key = service + ":" + version;
        synchronized (lock) {
            return loadBalance.selector(cacheMap.get(key));
        }
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
