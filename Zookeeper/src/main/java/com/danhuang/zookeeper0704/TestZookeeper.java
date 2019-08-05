package com.danhuang.zookeeper0704;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class TestZookeeper {

    private String connectString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
    private int sessionTimeout = 2000;
    private ZooKeeper zkClient;

    @Before
    public void init() throws IOException {
        zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            public void process(WatchedEvent watchedEvent) {
//                System.out.println("---------start-------------");
//                List<String> children = null;
//                try {
//                    children = zkClient.getChildren("/", true);
//                    for(String child : children){
//                        System.out.println(child);
//                    }
//                    System.out.println("-------------end--------------");
//                } catch (KeeperException e) {
//                    e.printStackTrace();
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
            }
        });
    }

    //1.创建节点
    @Test
    public void createNode() throws KeeperException, InterruptedException {
        String path = zkClient.create("/danhuang", "dahahigezuishuai".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
        System.out.println();
    }

    //2.获取子节点 并监控节点的变化
    @Test
    public void getDataAndWatch() throws KeeperException, InterruptedException {


        List<String> children = zkClient.getChildren("/", true);
        for(String child : children){
            System.out.println(child);
        }

        //延时阻塞
        Thread.sleep(Long.MAX_VALUE);

    }

    //判断节点是否存在
    @Test
    public void exist() throws KeeperException, InterruptedException {

        Stat stat = zkClient.exists("/danhuang", false);
        System.out.println(stat == null ? "not exist":"exist");
    }

}
